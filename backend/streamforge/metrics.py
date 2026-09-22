"""
StreamForge Metrics

Application metrics and monitoring support for the StreamForge
stream-processing system.

Metrics are exposed using the Prometheus client library.

Main responsibilities:

    - Track Kafka messages
    - Track processing throughput
    - Track processing errors
    - Track aggregation output
    - Track worker health
    - Track latency
    - Expose Prometheus metrics
    - Provide a simple metrics HTTP endpoint
"""

from __future__ import annotations

import logging
import threading
import time

from dataclasses import dataclass
from http.server import (
    BaseHTTPRequestHandler,
    HTTPServer,
)
from typing import Any, Dict, Optional

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


logger = logging.getLogger(__name__)


# ============================================================
# METRIC DEFINITIONS
# ============================================================


MESSAGES_RECEIVED = Counter(
    "streamforge_messages_received_total",
    "Total number of telemetry messages received.",
    ["worker_id"],
)


MESSAGES_PROCESSED = Counter(
    "streamforge_messages_processed_total",
    "Total number of telemetry messages processed successfully.",
    ["worker_id"],
)


MESSAGES_FAILED = Counter(
    "streamforge_messages_failed_total",
    "Total number of telemetry messages that failed processing.",
    ["worker_id"],
)


MESSAGES_INVALID = Counter(
    "streamforge_messages_invalid_total",
    "Total number of invalid telemetry messages.",
    ["worker_id"],
)


AGGREGATES_PRODUCED = Counter(
    "streamforge_aggregates_produced_total",
    "Total number of aggregate records produced.",
    ["worker_id"],
)


KAFKA_ERRORS = Counter(
    "streamforge_kafka_errors_total",
    "Total number of Kafka errors.",
    ["worker_id"],
)


PROCESSING_ERRORS = Counter(
    "streamforge_processing_errors_total",
    "Total number of processing errors.",
    ["worker_id"],
)


BYTES_RECEIVED = Counter(
    "streamforge_bytes_received_total",
    "Total number of bytes received from Kafka.",
    ["worker_id"],
)


BYTES_PRODUCED = Counter(
    "streamforge_bytes_produced_total",
    "Total number of bytes produced to Kafka.",
    ["worker_id"],
)


PROCESSING_TIME = Histogram(
    "streamforge_processing_seconds",
    "Telemetry event processing duration in seconds.",
    ["worker_id"],
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
    ),
)


WORKER_UP = Gauge(
    "streamforge_worker_up",
    "Whether the StreamForge worker is currently running.",
    ["worker_id"],
)


WORKER_MESSAGES_PER_SECOND = Gauge(
    "streamforge_worker_messages_per_second",
    "Current worker message processing rate.",
    ["worker_id"],
)


WORKER_LAST_SUCCESS_TIMESTAMP = Gauge(
    "streamforge_worker_last_success_timestamp",
    "Unix timestamp of the last successfully processed event.",
    ["worker_id"],
)


WORKER_RUNTIME_SECONDS = Gauge(
    "streamforge_worker_runtime_seconds",
    "Worker runtime in seconds.",
    ["worker_id"],
)


ACTIVE_WORKERS = Gauge(
    "streamforge_active_workers",
    "Number of currently active StreamForge workers.",
)


# ============================================================
# WORKER METRICS SNAPSHOT
# ============================================================


@dataclass
class MetricsSnapshot:
    """In-memory metrics snapshot for one worker."""

    worker_id: str

    messages_received: int = 0

    messages_processed: int = 0

    messages_failed: int = 0

    messages_invalid: int = 0

    aggregates_produced: int = 0

    kafka_errors: int = 0

    processing_errors: int = 0

    bytes_received: int = 0

    bytes_produced: int = 0

    processing_time_seconds: float = 0.0

    started_at: Optional[float] = None

    last_success_time: Optional[float] = None

    running: bool = False

    def to_dict(
        self,
    ) -> Dict[str, Any]:
        """Convert snapshot into a dictionary."""

        now = time.time()

        if self.started_at is not None:

            runtime = max(
                0.0,
                now - self.started_at,
            )

        else:

            runtime = 0.0

        if runtime > 0:

            messages_per_second = (
                self.messages_processed
                / runtime
            )

        else:

            messages_per_second = 0.0

        if self.messages_processed > 0:

            average_processing_ms = (
                self.processing_time_seconds
                / self.messages_processed
                * 1000.0
            )

        else:

            average_processing_ms = 0.0

        return {
            "worker_id":
                self.worker_id,

            "messages_received":
                self.messages_received,

            "messages_processed":
                self.messages_processed,

            "messages_failed":
                self.messages_failed,

            "messages_invalid":
                self.messages_invalid,

            "aggregates_produced":
                self.aggregates_produced,

            "kafka_errors":
                self.kafka_errors,

            "processing_errors":
                self.processing_errors,

            "bytes_received":
                self.bytes_received,

            "bytes_produced":
                self.bytes_produced,

            "processing_time_seconds":
                self.processing_time_seconds,

            "average_processing_ms":
                average_processing_ms,

            "messages_per_second":
                messages_per_second,

            "started_at":
                self.started_at,

            "last_success_time":
                self.last_success_time,

            "runtime_seconds":
                runtime,

            "running":
                self.running,
        }


# ============================================================
# METRICS MANAGER
# ============================================================


class MetricsManager:
    """
    Central metrics manager for StreamForge.

    The manager keeps lightweight worker snapshots while also
    updating Prometheus metrics.
    """

    def __init__(
        self,
        enabled: bool = True,
    ) -> None:

        self.enabled = enabled

        self._lock = threading.RLock()

        self._workers: Dict[
            str,
            MetricsSnapshot,
        ] = {}

        logger.info(
            "MetricsManager initialized: enabled=%s",
            self.enabled,
        )

    # ========================================================
    # WORKER REGISTRATION
    # ========================================================

    def register_worker(
        self,
        worker_id: str,
    ) -> None:
        """Register a worker."""

        if not worker_id:

            raise ValueError(
                "worker_id must not be empty."
            )

        with self._lock:

            if worker_id not in self._workers:

                self._workers[
                    worker_id
                ] = MetricsSnapshot(
                    worker_id=worker_id
                )

            snapshot = self._workers[
                worker_id
            ]

            snapshot.started_at = (
                time.time()
            )

            snapshot.running = True

            if self.enabled:

                WORKER_UP.labels(
                    worker_id=worker_id
                ).set(1)

                ACTIVE_WORKERS.inc()

        logger.info(
            "Worker registered for metrics: %s",
            worker_id,
        )

    # ========================================================
    # WORKER UNREGISTER
    # ========================================================

    def unregister_worker(
        self,
        worker_id: str,
    ) -> None:
        """Mark a worker as stopped."""

        with self._lock:

            snapshot = self._workers.get(
                worker_id
            )

            if snapshot is None:

                return

            was_running = (
                snapshot.running
            )

            snapshot.running = False

            if self.enabled:

                WORKER_UP.labels(
                    worker_id=worker_id
                ).set(0)

                if was_running:

                    ACTIVE_WORKERS.dec()

        logger.info(
            "Worker unregistered from metrics: %s",
            worker_id,
        )

    # ========================================================
    # MESSAGE RECEIVED
    # ========================================================

    def record_message_received(
        self,
        worker_id: str,
        bytes_count: int = 0,
    ) -> None:
        """Record a received telemetry message."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.messages_received += 1

            snapshot.bytes_received += max(
                0,
                bytes_count,
            )

            if self.enabled:

                MESSAGES_RECEIVED.labels(
                    worker_id=worker_id
                ).inc()

                if bytes_count > 0:

                    BYTES_RECEIVED.labels(
                        worker_id=worker_id
                    ).inc(
                        bytes_count
                    )

    # ========================================================
    # MESSAGE PROCESSED
    # ========================================================

    def record_message_processed(
        self,
        worker_id: str,
        processing_seconds: float = 0.0,
    ) -> None:
        """Record a successfully processed message."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.messages_processed += 1

            snapshot.processing_time_seconds += max(
                0.0,
                processing_seconds,
            )

            snapshot.last_success_time = (
                time.time()
            )

            self._update_rate(
                snapshot
            )

            if self.enabled:

                MESSAGES_PROCESSED.labels(
                    worker_id=worker_id
                ).inc()

                if processing_seconds > 0:

                    PROCESSING_TIME.labels(
                        worker_id=worker_id
                    ).observe(
                        processing_seconds
                    )

                WORKER_LAST_SUCCESS_TIMESTAMP.labels(
                    worker_id=worker_id
                ).set(
                    snapshot.last_success_time
                )

    # ========================================================
    # MESSAGE FAILED
    # ========================================================

    def record_message_failed(
        self,
        worker_id: str,
    ) -> None:
        """Record a failed telemetry message."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.messages_failed += 1

            if self.enabled:

                MESSAGES_FAILED.labels(
                    worker_id=worker_id
                ).inc()

    # ========================================================
    # INVALID MESSAGE
    # ========================================================

    def record_invalid_message(
        self,
        worker_id: str,
    ) -> None:
        """Record an invalid telemetry message."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.messages_invalid += 1

            if self.enabled:

                MESSAGES_INVALID.labels(
                    worker_id=worker_id
                ).inc()

    # ========================================================
    # AGGREGATE PRODUCED
    # ========================================================

    def record_aggregate_produced(
        self,
        worker_id: str,
        bytes_count: int = 0,
    ) -> None:
        """Record an aggregate output."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.aggregates_produced += 1

            snapshot.bytes_produced += max(
                0,
                bytes_count,
            )

            if self.enabled:

                AGGREGATES_PRODUCED.labels(
                    worker_id=worker_id
                ).inc()

                if bytes_count > 0:

                    BYTES_PRODUCED.labels(
                        worker_id=worker_id
                    ).inc(
                        bytes_count
                    )

    # ========================================================
    # KAFKA ERROR
    # ========================================================

    def record_kafka_error(
        self,
        worker_id: str,
    ) -> None:
        """Record a Kafka error."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.kafka_errors += 1

            if self.enabled:

                KAFKA_ERRORS.labels(
                    worker_id=worker_id
                ).inc()

    # ========================================================
    # PROCESSING ERROR
    # ========================================================

    def record_processing_error(
        self,
        worker_id: str,
    ) -> None:
        """Record a processing error."""

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.processing_errors += 1

            if self.enabled:

                PROCESSING_ERRORS.labels(
                    worker_id=worker_id
                ).inc()

    # ========================================================
    # UPDATE WORKER METRICS
    # ========================================================

    def update_worker(
        self,
        worker_id: str,
        statistics: Dict[str, Any],
    ) -> None:
        """
        Update the worker snapshot from a worker statistics
        dictionary.

        This allows worker.py to periodically synchronize its
        counters with MetricsManager.
        """

        with self._lock:

            snapshot = self._get_or_create(
                worker_id
            )

            snapshot.messages_received = int(
                statistics.get(
                    "messages_received",
                    snapshot.messages_received,
                )
            )

            snapshot.messages_processed = int(
                statistics.get(
                    "messages_processed",
                    snapshot.messages_processed,
                )
            )

            snapshot.messages_failed = int(
                statistics.get(
                    "messages_failed",
                    snapshot.messages_failed,
                )
            )

            snapshot.messages_invalid = int(
                statistics.get(
                    "messages_invalid",
                    snapshot.messages_invalid,
                )
            )

            snapshot.aggregates_produced = int(
                statistics.get(
                    "aggregates_produced",
                    snapshot.aggregates_produced,
                )
            )

            snapshot.kafka_errors = int(
                statistics.get(
                    "kafka_errors",
                    snapshot.kafka_errors,
                )
            )

            snapshot.processing_errors = int(
                statistics.get(
                    "processing_errors",
                    snapshot.processing_errors,
                )
            )

            snapshot.bytes_received = int(
                statistics.get(
                    "bytes_received",
                    snapshot.bytes_received,
                )
            )

            snapshot.bytes_produced = int(
                statistics.get(
                    "bytes_produced",
                    snapshot.bytes_produced,
                )
            )

            snapshot.processing_time_seconds = float(
                statistics.get(
                    "processing_time_seconds",
                    snapshot.processing_time_seconds,
                )
            )

            snapshot.started_at = (
                statistics.get(
                    "started_at",
                    snapshot.started_at,
                )
            )

            snapshot.last_success_time = (
                statistics.get(
                    "last_success_time",
                    snapshot.last_success_time,
                )
            )

            snapshot.running = bool(
                statistics.get(
                    "running",
                    snapshot.running,
                )
            )

            self._update_rate(
                snapshot
            )

            if self.enabled:

                WORKER_UP.labels(
                    worker_id=worker_id
                ).set(
                    1 if snapshot.running else 0
                )

                if snapshot.started_at is not None:

                    WORKER_RUNTIME_SECONDS.labels(
                        worker_id=worker_id
                    ).set(
                        max(
                            0.0,
                            time.time()
                            - snapshot.started_at,
                        )
                    )

    # ========================================================
    # UPDATE RATE
    # ========================================================

    def _update_rate(
        self,
        snapshot: MetricsSnapshot,
    ) -> None:
        """Update worker throughput gauge."""

        if snapshot.started_at is None:

            return

        runtime = max(
            0.0,
            time.time()
            - snapshot.started_at,
        )

        if runtime <= 0:

            rate = 0.0

        else:

            rate = (
                snapshot.messages_processed
                / runtime
            )

        if self.enabled:

            WORKER_MESSAGES_PER_SECOND.labels(
                worker_id=snapshot.worker_id
            ).set(
                rate
            )

            WORKER_RUNTIME_SECONDS.labels(
                worker_id=snapshot.worker_id
            ).set(
                runtime
            )

    # ========================================================
    # INTERNAL GET
    # ========================================================

    def _get_or_create(
        self,
        worker_id: str,
    ) -> MetricsSnapshot:
        """Get or create a worker snapshot."""

        snapshot = self._workers.get(
            worker_id
        )

        if snapshot is None:

            snapshot = MetricsSnapshot(
                worker_id=worker_id
            )

            self._workers[
                worker_id
            ] = snapshot

        return snapshot

    # ========================================================
    # WORKER SNAPSHOT
    # ========================================================

    def get_worker(
        self,
        worker_id: str,
    ) -> Optional[
        Dict[str, Any]
    ]:
        """Return metrics for one worker."""

        with self._lock:

            snapshot = self._workers.get(
                worker_id
            )

            if snapshot is None:

                return None

            self._update_rate(
                snapshot
            )

            return snapshot.to_dict()

    # ========================================================
    # ALL WORKERS
    # ========================================================

    def get_workers(
        self,
    ) -> Dict[
        str,
        Dict[str, Any],
    ]:
        """Return metrics for all workers."""

        with self._lock:

            result = {}

            for (
                worker_id,
                snapshot,
            ) in self._workers.items():

                self._update_rate(
                    snapshot
                )

                result[
                    worker_id
                ] = snapshot.to_dict()

            return result

    # ========================================================
    # SUMMARY
    # ========================================================

    def get_summary(
        self,
    ) -> Dict[str, Any]:
        """Return aggregate metrics for the application."""

        with self._lock:

            workers = self.get_workers()

            total_received = sum(
                worker[
                    "messages_received"
                ]
                for worker in workers.values()
            )

            total_processed = sum(
                worker[
                    "messages_processed"
                ]
                for worker in workers.values()
            )

            total_failed = sum(
                worker[
                    "messages_failed"
                ]
                for worker in workers.values()
            )

            total_invalid = sum(
                worker[
                    "messages_invalid"
                ]
                for worker in workers.values()
            )

            total_aggregates = sum(
                worker[
                    "aggregates_produced"
                ]
                for worker in workers.values()
            )

            total_kafka_errors = sum(
                worker[
                    "kafka_errors"
                ]
                for worker in workers.values()
            )

            total_processing_errors = sum(
                worker[
                    "processing_errors"
                ]
                for worker in workers.values()
            )

            total_bytes_received = sum(
                worker[
                    "bytes_received"
                ]
                for worker in workers.values()
            )

            total_bytes_produced = sum(
                worker[
                    "bytes_produced"
                ]
                for worker in workers.values()
            )

            active_workers = sum(
                1
                for worker in workers.values()
                if worker["running"]
            )

            return {
                "workers":
                    len(workers),

                "active_workers":
                    active_workers,

                "messages_received":
                    total_received,

                "messages_processed":
                    total_processed,

                "messages_failed":
                    total_failed,

                "messages_invalid":
                    total_invalid,

                "aggregates_produced":
                    total_aggregates,

                "kafka_errors":
                    total_kafka_errors,

                "processing_errors":
                    total_processing_errors,

                "bytes_received":
                    total_bytes_received,

                "bytes_produced":
                    total_bytes_produced,
            }

    # ========================================================
    # RESET
    # ========================================================

    def reset_worker(
        self,
        worker_id: str,
    ) -> None:
        """Reset the local snapshot for a worker."""

        with self._lock:

            self._workers[
                worker_id
            ] = MetricsSnapshot(
                worker_id=worker_id
            )

    # ========================================================
    # CLEAR
    # ========================================================

    def clear(
        self,
    ) -> None:
        """Clear all local worker snapshots."""

        with self._lock:

            self._workers.clear()

    # ========================================================
    # PROMETHEUS OUTPUT
    # ========================================================

    def prometheus_output(
        self,
    ) -> bytes:
        """Return Prometheus exposition data."""

        if not self.enabled:

            return (
                b"# StreamForge metrics disabled\n"
            )

        return generate_latest()

    # ========================================================
    # HEALTH
    # ========================================================

    def health(
        self,
    ) -> Dict[str, Any]:
        """Return metrics subsystem health."""

        with self._lock:

            return {
                "enabled":
                    self.enabled,

                "workers_registered":
                    len(self._workers),

                "active_workers":
                    sum(
                        1
                        for worker in self._workers.values()
                        if worker.running
                    ),
            }


# ============================================================
# PROMETHEUS HTTP SERVER
# ============================================================


class _MetricsRequestHandler(
    BaseHTTPRequestHandler
):
    """HTTP handler for Prometheus metrics."""

    metrics_manager: Optional[
        MetricsManager
    ] = None

    def do_GET(
        self,
    ) -> None:

        if self.path not in (
            "/metrics",
            "/",
        ):

            self.send_response(
                404
            )

            self.send_header(
                "Content-Type",
                "text/plain; charset=utf-8",
            )

            self.end_headers()

            self.wfile.write(
                b"Not Found\n"
            )

            return

        manager = (
            self.metrics_manager
        )

        if manager is None:

            body = (
                b"Metrics manager unavailable.\n"
            )

            content_type = (
                "text/plain; charset=utf-8"
            )

        else:

            body = (
                manager.prometheus_output()
            )

            content_type = (
                CONTENT_TYPE_LATEST
            )

        self.send_response(
            200
        )

        self.send_header(
            "Content-Type",
            content_type,
        )

        self.send_header(
            "Content-Length",
            str(len(body)),
        )

        self.end_headers()

        self.wfile.write(
            body
        )

    def log_message(
        self,
        format: str,
        *args: Any,
    ) -> None:

        logger.debug(
            "Metrics HTTP: %s",
            format % args,
        )


class MetricsServer:
    """
    Small HTTP server exposing Prometheus metrics.
    """

    def __init__(
        self,
        metrics_manager: MetricsManager,
        host: str = "127.0.0.1",
        port: int = 8001,
    ) -> None:

        self.metrics_manager = (
            metrics_manager
        )

        self.host = host

        self.port = port

        self._server: Optional[
            HTTPServer
        ] = None

        self._thread: Optional[
            threading.Thread
        ] = None

    # ========================================================
    # START
    # ========================================================

    def start(
        self,
    ) -> None:

        if self._server is not None:

            return

        handler_class = (
            type(
                "StreamForgeMetricsHandler",
                (_MetricsRequestHandler,),
                {
                    "metrics_manager":
                        self.metrics_manager
                },
            )
        )

        self._server = HTTPServer(
            (
                self.host,
                self.port,
            ),
            handler_class,
        )

        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="streamforge-metrics",
            daemon=True,
        )

        self._thread.start()

        logger.info(
            "StreamForge metrics server started at "
            "http://%s:%d/metrics",
            self.host,
            self.port,
        )

    # ========================================================
    # STOP
    # ========================================================

    def stop(
        self,
    ) -> None:

        if self._server is None:

            return

        try:

            self._server.shutdown()

            self._server.server_close()

        finally:

            self._server = None

            self._thread = None

        logger.info(
            "StreamForge metrics server stopped."
        )

    # ========================================================
    # RUNNING
    # ========================================================

    @property
    def running(
        self,
    ) -> bool:

        return self._server is not None


# ============================================================
# GLOBAL METRICS MANAGER
# ============================================================


metrics_manager = MetricsManager()


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================


def record_message_received(
    worker_id: str,
    bytes_count: int = 0,
) -> None:

    metrics_manager.record_message_received(
        worker_id,
        bytes_count,
    )


def record_message_processed(
    worker_id: str,
    processing_seconds: float = 0.0,
) -> None:

    metrics_manager.record_message_processed(
        worker_id,
        processing_seconds,
    )


def record_message_failed(
    worker_id: str,
) -> None:

    metrics_manager.record_message_failed(
        worker_id
    )


def record_invalid_message(
    worker_id: str,
) -> None:

    metrics_manager.record_invalid_message(
        worker_id
    )


def record_aggregate_produced(
    worker_id: str,
    bytes_count: int = 0,
) -> None:

    metrics_manager.record_aggregate_produced(
        worker_id,
        bytes_count,
    )


def record_kafka_error(
    worker_id: str,
) -> None:

    metrics_manager.record_kafka_error(
        worker_id
    )


def record_processing_error(
    worker_id: str,
) -> None:

    metrics_manager.record_processing_error(
        worker_id
    )


# ============================================================
# SELF TEST
# ============================================================


def run_self_test() -> None:
    """Run a complete metrics self-test."""

    print(
        "StreamForge metrics self-test"
    )

    print(
        "-" * 60
    )

    manager = MetricsManager(
        enabled=True
    )

    worker_id = (
        "self-test-worker"
    )

    # --------------------------------------------------------
    # Worker registration
    # --------------------------------------------------------

    manager.register_worker(
        worker_id
    )

    health = manager.health()

    assert (
        health["enabled"]
        is True
    )

    assert (
        health["workers_registered"]
        == 1
    )

    assert (
        health["active_workers"]
        == 1
    )

    print(
        "Worker registration: OK"
    )

    # --------------------------------------------------------
    # Message received
    # --------------------------------------------------------

    manager.record_message_received(
        worker_id,
        bytes_count=100,
    )

    manager.record_message_received(
        worker_id,
        bytes_count=200,
    )

    worker = manager.get_worker(
        worker_id
    )

    assert worker is not None

    assert (
        worker[
            "messages_received"
        ]
        == 2
    )

    assert (
        worker[
            "bytes_received"
        ]
        == 300
    )

    print(
        "Message counters: OK"
    )

    # --------------------------------------------------------
    # Processed messages
    # --------------------------------------------------------

    manager.record_message_processed(
        worker_id,
        processing_seconds=0.01,
    )

    manager.record_message_processed(
        worker_id,
        processing_seconds=0.02,
    )

    worker = manager.get_worker(
        worker_id
    )

    assert worker is not None

    assert (
        worker[
            "messages_processed"
        ]
        == 2
    )

    assert (
        worker[
            "processing_time_seconds"
        ]
        > 0
    )

    print(
        "Processing metrics: OK"
    )

    # --------------------------------------------------------
    # Errors
    # --------------------------------------------------------

    manager.record_message_failed(
        worker_id
    )

    manager.record_invalid_message(
        worker_id
    )

    manager.record_kafka_error(
        worker_id
    )

    manager.record_processing_error(
        worker_id
    )

    worker = manager.get_worker(
        worker_id
    )

    assert worker is not None

    assert (
        worker[
            "messages_failed"
        ]
        == 1
    )

    assert (
        worker[
            "messages_invalid"
        ]
        == 1
    )

    assert (
        worker[
            "kafka_errors"
        ]
        == 1
    )

    assert (
        worker[
            "processing_errors"
        ]
        == 1
    )

    print(
        "Error metrics: OK"
    )

    # --------------------------------------------------------
    # Aggregate output
    # --------------------------------------------------------

    manager.record_aggregate_produced(
        worker_id,
        bytes_count=150,
    )

    worker = manager.get_worker(
        worker_id
    )

    assert worker is not None

    assert (
        worker[
            "aggregates_produced"
        ]
        == 1
    )

    assert (
        worker[
            "bytes_produced"
        ]
        == 150
    )

    print(
        "Aggregate metrics: OK"
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    summary = (
        manager.get_summary()
    )

    assert (
        summary[
            "workers"
        ]
        == 1
    )

    assert (
        summary[
            "active_workers"
        ]
        == 1
    )

    assert (
        summary[
            "messages_received"
        ]
        == 2
    )

    assert (
        summary[
            "messages_processed"
        ]
        == 2
    )

    assert (
        summary[
            "aggregates_produced"
        ]
        == 1
    )

    print(
        "Metrics summary: OK"
    )

    # --------------------------------------------------------
    # Prometheus output
    # --------------------------------------------------------

    prometheus_data = (
        manager.prometheus_output()
    )

    assert isinstance(
        prometheus_data,
        bytes,
    )

    assert (
        b"streamforge_messages_received_total"
        in prometheus_data
    )

    assert (
        b"streamforge_messages_processed_total"
        in prometheus_data
    )

    print(
        "Prometheus output: OK"
    )

    # --------------------------------------------------------
    # Worker unregister
    # --------------------------------------------------------

    manager.unregister_worker(
        worker_id
    )

    health = manager.health()

    assert (
        health["active_workers"]
        == 0
    )

    print(
        "Worker shutdown metrics: OK"
    )

    # --------------------------------------------------------
    # Multiple workers
    # --------------------------------------------------------

    manager.register_worker(
        "worker-1"
    )

    manager.register_worker(
        "worker-2"
    )

    manager.record_message_received(
        "worker-1"
    )

    manager.record_message_received(
        "worker-2"
    )

    workers = (
        manager.get_workers()
    )

    assert (
        len(workers)
        == 3
    )

    print(
        "Multi-worker metrics: OK"
    )

    manager.clear()

    assert (
        len(
            manager.get_workers()
        )
        == 0
    )

    print(
        "Metrics cleanup: OK"
    )

    # --------------------------------------------------------
    # Final result
    # --------------------------------------------------------

    print(
        "-" * 60
    )

    print(
        "StreamForge metrics self-test: OK"
    )


# ============================================================
# MAIN
# ============================================================


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    run_self_test()


if __name__ == "__main__":

    main()