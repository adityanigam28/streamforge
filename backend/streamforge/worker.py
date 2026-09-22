"""
StreamForge Worker
==================

Main runtime worker for the StreamForge streaming pipeline.

Pipeline:

    Kafka telemetry
          |
          v
       Worker
          |
          v
 TemperatureAggregator
          |
          +------> StateStore
          |
          v
 temperature-aggregates

Responsibilities:
    - Consume telemetry events from Kafka
    - Validate telemetry events
    - Process events through TemperatureAggregator
    - Publish aggregate results
    - Publish worker metrics
    - Maintain runtime statistics
    - Graceful shutdown
    - Commit Kafka offsets only after successful processing
"""

from __future__ import annotations

import json
import logging
import signal
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from .aggregator import TemperatureAggregator
from .config import settings
from .models import TelemetryEvent
from .state import StateStore


logger = logging.getLogger(__name__)


# ============================================================
# DEFAULTS
# ============================================================

DEFAULT_MAX_POLL_RECORDS = 500
DEFAULT_POLL_TIMEOUT_MS = 1000
DEFAULT_REQUEST_TIMEOUT_MS = 30000
DEFAULT_SESSION_TIMEOUT_MS = 10000


# ============================================================
# EXCEPTIONS
# ============================================================


class WorkerError(Exception):
    """Base exception for worker errors."""


class WorkerConfigurationError(WorkerError):
    """Raised when worker configuration is invalid."""


class TelemetryProcessingError(WorkerError):
    """Raised when telemetry processing fails."""


# ============================================================
# STATISTICS
# ============================================================


@dataclass
class WorkerStatistics:
    """Runtime statistics for one worker."""

    worker_id: str

    started_at: Optional[float] = None
    stopped_at: Optional[float] = None

    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    messages_invalid: int = 0

    aggregates_produced: int = 0

    bytes_received: int = 0
    bytes_produced: int = 0

    processing_time_seconds: float = 0.0

    kafka_errors: int = 0
    processing_errors: int = 0

    last_event_time: Optional[float] = None
    last_success_time: Optional[float] = None
    last_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return statistics as a dictionary."""

        if self.started_at is not None:
            end_time = (
                self.stopped_at
                if self.stopped_at is not None
                else time.time()
            )

            runtime = max(
                0.0,
                end_time - self.started_at,
            )
        else:
            runtime = 0.0

        if runtime > 0:
            messages_per_second = (
                self.messages_processed / runtime
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
            "worker_id": self.worker_id,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "runtime_seconds": runtime,
            "messages_received": self.messages_received,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "messages_invalid": self.messages_invalid,
            "aggregates_produced": self.aggregates_produced,
            "bytes_received": self.bytes_received,
            "bytes_produced": self.bytes_produced,
            "processing_time_seconds": (
                self.processing_time_seconds
            ),
            "average_processing_ms": (
                average_processing_ms
            ),
            "messages_per_second": (
                messages_per_second
            ),
            "kafka_errors": self.kafka_errors,
            "processing_errors": self.processing_errors,
            "last_event_time": self.last_event_time,
            "last_success_time": self.last_success_time,
            "last_error": self.last_error,
        }


# ============================================================
# WORKER
# ============================================================


class StreamForgeWorker:
    """Main StreamForge Kafka processing worker."""

    def __init__(
        self,
        worker_id: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        telemetry_topic: Optional[str] = None,
        aggregate_topic: Optional[str] = None,
        metrics_topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        state_directory: Optional[str | Path] = None,
    ) -> None:

        # ----------------------------------------------------
        # Read current StreamForge configuration
        # ----------------------------------------------------

        self.worker_id = (
            worker_id
            or f"worker-{uuid.uuid4().hex[:8]}"
        )

        self.bootstrap_servers = (
            bootstrap_servers
            or settings.kafka.bootstrap_servers
        )

        self.telemetry_topic = (
            telemetry_topic
            or settings.kafka.telemetry_topic
        )

        self.aggregate_topic = (
            aggregate_topic
            or settings.kafka.temperature_aggregates_topic
        )

        self.metrics_topic = (
            metrics_topic
            or settings.kafka.worker_metrics_topic
        )

        self.consumer_group = (
            consumer_group
            or settings.kafka.consumer_group
        )

        configured_state_directory = getattr(
            settings.state,
            "directory",
            Path("data") / "state",
        )

        self.state_directory = Path(
            state_directory
            or (
                Path(configured_state_directory)
                / self.worker_id
            )
        )

        self.state_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        # ----------------------------------------------------
        # Runtime Kafka settings
        # ----------------------------------------------------

        self.max_poll_records = getattr(
            settings.worker,
            "max_poll_records",
            DEFAULT_MAX_POLL_RECORDS,
        )

        self.poll_timeout_ms = getattr(
            settings.worker,
            "poll_timeout_ms",
            DEFAULT_POLL_TIMEOUT_MS,
        )

        # ----------------------------------------------------
        # Runtime components
        # ----------------------------------------------------

        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None
        self._state_store: Optional[StateStore] = None
        self._aggregator: Optional[TemperatureAggregator] = None

        self._statistics = WorkerStatistics(
            worker_id=self.worker_id
        )

        self._running = False
        self._initialized = False

        self._shutdown_event = threading.Event()

        logger.info(
            "StreamForgeWorker created: worker_id=%s",
            self.worker_id,
        )

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def initialize(self) -> None:
        """Initialize Kafka, state store and aggregator."""

        if self._initialized:
            return

        logger.info(
            "Initializing worker: %s",
            self.worker_id,
        )

        try:
            # ------------------------------------------------
            # Kafka consumer
            # ------------------------------------------------

            self._consumer = KafkaConsumer(
                self.telemetry_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                client_id=(
                    f"streamforge-{self.worker_id}"
                ),
                auto_offset_reset="earliest",

                # IMPORTANT:
                # Offsets are committed manually after
                # successful message processing.
                enable_auto_commit=False,

                max_poll_records=self.max_poll_records,

                value_deserializer=(
                    lambda value:
                    json.loads(
                        value.decode("utf-8")
                    )
                ),

                request_timeout_ms=(
                    DEFAULT_REQUEST_TIMEOUT_MS
                ),

                session_timeout_ms=(
                    DEFAULT_SESSION_TIMEOUT_MS
                ),
            )

            logger.info(
                "Kafka consumer initialized: topic=%s group=%s",
                self.telemetry_topic,
                self.consumer_group,
            )

            # ------------------------------------------------
            # Kafka producer
            # ------------------------------------------------

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,

                client_id=(
                    f"streamforge-{self.worker_id}-producer"
                ),

                acks="all",
                retries=5,
                linger_ms=5,

                key_serializer=(
                    lambda key:
                    key.encode("utf-8")
                    if isinstance(key, str)
                    else key
                ),

                value_serializer=(
                    lambda value:
                    json.dumps(
                        value,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                ),
            )

            logger.info(
                "Kafka producer initialized."
            )

            # ------------------------------------------------
            # State store
            # ------------------------------------------------

            self._state_store = StateStore(
                name=self.worker_id,
                directory=self.state_directory,
            )

            logger.info(
                "State store initialized: %s",
                self.state_directory,
            )

            # ------------------------------------------------
            # Aggregator
            # ------------------------------------------------

            self._aggregator = TemperatureAggregator(
                state_store=self._state_store
            )

            logger.info(
                "Temperature aggregator initialized."
            )

            self._initialized = True

            logger.info(
                "StreamForge worker initialized successfully: "
                "%s",
                self.worker_id,
            )

        except Exception as exc:
            logger.exception(
                "Worker initialization failed."
            )

            self.close()

            raise WorkerError(
                f"Failed to initialize worker: {exc}"
            ) from exc

    # ========================================================
    # EVENT DECODING
    # ========================================================

    @staticmethod
    def decode_event(
        value: Any,
    ) -> Dict[str, Any]:
        """Decode a telemetry payload."""

        if isinstance(value, dict):
            return value

        if isinstance(value, bytes):
            try:
                decoded = json.loads(
                    value.decode("utf-8")
                )
            except json.JSONDecodeError as exc:
                raise TelemetryProcessingError(
                    "Invalid JSON telemetry."
                ) from exc

            if not isinstance(decoded, dict):
                raise TelemetryProcessingError(
                    "Telemetry JSON must be an object."
                )

            return decoded

        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError as exc:
                raise TelemetryProcessingError(
                    "Invalid JSON telemetry."
                ) from exc

            if not isinstance(decoded, dict):
                raise TelemetryProcessingError(
                    "Telemetry JSON must be an object."
                )

            return decoded

        raise TelemetryProcessingError(
            "Unsupported telemetry payload type: "
            f"{type(value).__name__}"
        )

    # ========================================================
    # VALIDATION
    # ========================================================

    @staticmethod
    def validate_event(
        payload: Dict[str, Any],
    ) -> TelemetryEvent:
        """Validate a telemetry event using Pydantic."""

        try:
            if hasattr(
                TelemetryEvent,
                "model_validate",
            ):
                return TelemetryEvent.model_validate(
                    payload
                )

            return TelemetryEvent.parse_obj(
                payload
            )

        except Exception as exc:
            raise TelemetryProcessingError(
                f"Invalid telemetry event: {exc}"
            ) from exc

    # ========================================================
    # OBJECT TO DICTIONARY
    # ========================================================

    @staticmethod
    def object_to_dict(
        value: Any,
    ) -> Optional[Dict[str, Any]]:
        """Convert supported objects into dictionaries."""

        if isinstance(value, dict):
            return value

        if hasattr(value, "model_dump"):
            result = value.model_dump()

            if isinstance(result, dict):
                return result

        if hasattr(value, "dict"):
            result = value.dict()

            if isinstance(result, dict):
                return result

        if hasattr(value, "__dict__"):
            result = vars(value)

            if isinstance(result, dict):
                return result

        return None

    # ========================================================
    # AGGREGATE KEY
    # ========================================================

    @staticmethod
    def build_aggregate_key(
        payload: Dict[str, Any],
    ) -> str:
        """
        Build a deterministic Kafka key.

        The temperature-aggregates topic is compacted, so
        records need stable keys.

        Primary key:

            device_id + window_start
        """

        device_id = payload.get("device_id")
        window_start = payload.get("window_start")

        if (
            device_id is not None
            and window_start is not None
        ):
            return f"{device_id}:{window_start}"

        aggregation_id = payload.get(
            "aggregation_id"
        )

        if aggregation_id is not None:
            return str(aggregation_id)

        return (
            "aggregate:"
            f"{payload.get('window_end', 'unknown')}"
        )

    # ========================================================
    # PROCESS EVENT
    # ========================================================

    def process_event(
        self,
        payload: Dict[str, Any],
    ) -> int:
        """
        Validate and process one telemetry event.

        Returns:
            Number of aggregate records produced.
        """

        if self._aggregator is None:
            raise WorkerError(
                "Worker has not been initialized."
            )

        event = self.validate_event(payload)

        try:
            # Current aggregator supports process_event().
            if hasattr(
                self._aggregator,
                "process_event",
            ):
                result = (
                    self._aggregator.process_event(
                        event
                    )
                )

            elif hasattr(
                self._aggregator,
                "process",
            ):
                result = (
                    self._aggregator.process(
                        event
                    )
                )

            elif hasattr(
                self._aggregator,
                "add_event",
            ):
                result = (
                    self._aggregator.add_event(
                        event
                    )
                )

            else:
                raise WorkerError(
                    "TemperatureAggregator does not "
                    "provide a supported processing method."
                )

        except Exception as exc:
            raise TelemetryProcessingError(
                f"Aggregator processing failed: {exc}"
            ) from exc

        return self.publish_aggregation_result(
            result
        )

    # ========================================================
    # PUBLISH AGGREGATION RESULT
    # ========================================================

    def publish_aggregation_result(
        self,
        result: Any,
    ) -> int:
        """Publish all aggregate records returned by aggregator."""

        if result is None:
            return 0

        if isinstance(result, (list, tuple)):
            results = list(result)
        else:
            results = [result]

        count = 0

        for item in results:
            if item is None:
                continue

            payload = self.object_to_dict(item)

            if payload is None:
                continue

            self.publish_aggregate(payload)
            count += 1

        return count

    # ========================================================
    # PUBLISH AGGREGATE
    # ========================================================

    def publish_aggregate(
        self,
        payload: Dict[str, Any],
    ) -> None:
        """Publish one aggregate record to Kafka."""

        if self._producer is None:
            raise WorkerError(
                "Kafka producer is not initialized."
            )

        try:
            aggregate_key = (
                self.build_aggregate_key(
                    payload
                )
            )

            future = self._producer.send(
                self.aggregate_topic,
                key=aggregate_key,
                value=payload,
            )

            metadata = future.get(
                timeout=10
            )

            payload_size = len(
                json.dumps(
                    payload,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            )

            self._statistics.bytes_produced += (
                payload_size
            )

            self._statistics.aggregates_produced += 1

            logger.debug(
                "Aggregate published: "
                "topic=%s partition=%s offset=%s key=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
                aggregate_key,
            )

        except Exception as exc:
            self._statistics.kafka_errors += 1
            self._statistics.last_error = str(exc)

            raise WorkerError(
                f"Failed to publish aggregate: {exc}"
            ) from exc

    # ========================================================
    # PROCESS MESSAGE
    # ========================================================

    def process_message(
        self,
        message: Any,
    ) -> bool:
        """
        Process one Kafka message.

        Returns:
            True when successfully processed.
            False when processing failed.
        """

        start = time.perf_counter()

        self._statistics.messages_received += 1

        try:
            payload = message.value

            # ------------------------------------------------
            # Count incoming bytes
            # ------------------------------------------------

            try:
                self._statistics.bytes_received += len(
                    json.dumps(
                        payload,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                )
            except Exception:
                pass

            # ------------------------------------------------
            # Decode
            # ------------------------------------------------

            decoded = self.decode_event(payload)

            # ------------------------------------------------
            # Process
            # ------------------------------------------------

            self.process_event(decoded)

            # ------------------------------------------------
            # Commit ONLY after successful processing
            # ------------------------------------------------

            if self._consumer is not None:
                self._consumer.commit()

            elapsed = (
                time.perf_counter() - start
            )

            self._statistics.processing_time_seconds += (
                elapsed
            )

            self._statistics.messages_processed += 1

            now = time.time()

            self._statistics.last_event_time = now
            self._statistics.last_success_time = now

            logger.debug(
                "Telemetry processed: "
                "worker=%s partition=%s offset=%s",
                self.worker_id,
                getattr(message, "partition", None),
                getattr(message, "offset", None),
            )

            return True

        except TelemetryProcessingError as exc:
            self._statistics.messages_invalid += 1
            self._statistics.messages_failed += 1
            self._statistics.processing_errors += 1
            self._statistics.last_error = str(exc)

            logger.error(
                "Telemetry processing error: %s",
                exc,
            )

            return False

        except Exception as exc:
            self._statistics.messages_failed += 1
            self._statistics.processing_errors += 1
            self._statistics.last_error = str(exc)

            logger.exception(
                "Unexpected worker error."
            )

            return False

    # ========================================================
    # RUN
    # ========================================================

    def run(self) -> None:
        """Run the Kafka consumer loop."""

        if not self._initialized:
            self.initialize()

        if self._consumer is None:
            raise WorkerError(
                "Kafka consumer is unavailable."
            )

        if self._running:
            logger.warning(
                "Worker is already running."
            )
            return

        self._running = True
        self._shutdown_event.clear()

        self._statistics.started_at = time.time()
        self._statistics.stopped_at = None

        logger.info(
            "StreamForge worker started: %s",
            self.worker_id,
        )

        try:
            while (
                self._running
                and not self._shutdown_event.is_set()
            ):

                try:
                    records = self._consumer.poll(
                        timeout_ms=self.poll_timeout_ms,
                        max_records=self.max_poll_records,
                    )

                except KafkaError as exc:
                    self._statistics.kafka_errors += 1
                    self._statistics.last_error = str(exc)

                    logger.error(
                        "Kafka polling error: %s",
                        exc,
                    )

                    time.sleep(1)
                    continue

                if not records:
                    continue

                for (
                    _partition,
                    messages,
                ) in records.items():

                    for message in messages:

                        if not self._running:
                            break

                        self.process_message(message)

        except KeyboardInterrupt:
            logger.info(
                "Keyboard interrupt received."
            )

        finally:
            self._running = False
            self._statistics.stopped_at = time.time()

            logger.info(
                "StreamForge worker stopped."
            )

    # ========================================================
    # STOP
    # ========================================================

    def stop(self) -> None:
        """Request graceful shutdown."""

        self._running = False
        self._shutdown_event.set()

        logger.info(
            "Worker shutdown requested: %s",
            self.worker_id,
        )

    # ========================================================
    # HEALTH
    # ========================================================

    def health(self) -> Dict[str, Any]:
        """Return worker health information."""

        return {
            "worker_id": self.worker_id,
            "initialized": self._initialized,
            "running": self._running,
            "kafka_consumer": (
                self._consumer is not None
            ),
            "kafka_producer": (
                self._producer is not None
            ),
            "state_store": (
                self._state_store is not None
            ),
            "aggregator": (
                self._aggregator is not None
            ),
        }

    # ========================================================
    # STATISTICS
    # ========================================================

    def get_statistics(self) -> Dict[str, Any]:
        """Return worker statistics."""

        return self._statistics.to_dict()

    # ========================================================
    # PUBLISH METRICS
    # ========================================================

    def publish_metrics(self) -> None:
        """Publish worker statistics to Kafka."""

        if self._producer is None:
            return

        payload = {
            "worker_id": self.worker_id,
            "timestamp": time.time(),
            "metrics": self.get_statistics(),
        }

        try:
            future = self._producer.send(
                self.metrics_topic,
                key=self.worker_id,
                value=payload,
            )

            # Ensure the metrics message was accepted.
            future.get(timeout=10)

        except Exception as exc:
            self._statistics.kafka_errors += 1
            self._statistics.last_error = str(exc)

            logger.error(
                "Failed to publish worker metrics: %s",
                exc,
            )

    # ========================================================
    # CLOSE
    # ========================================================

    def close(self) -> None:
        """Close all worker resources safely."""

        self.stop()

        # ----------------------------------------------------
        # Producer
        # ----------------------------------------------------

        if self._producer is not None:
            try:
                self._producer.flush(
                    timeout=10
                )
            except Exception as exc:
                logger.warning(
                    "Producer flush failed: %s",
                    exc,
                )

            try:
                self._producer.close(
                    timeout=10
                )
            except Exception as exc:
                logger.warning(
                    "Producer close failed: %s",
                    exc,
                )

            self._producer = None

        # ----------------------------------------------------
        # Consumer
        # ----------------------------------------------------

        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as exc:
                logger.warning(
                    "Consumer close failed: %s",
                    exc,
                )

            self._consumer = None

        # ----------------------------------------------------
        # State store
        # ----------------------------------------------------

        if self._state_store is not None:
            try:
                self._state_store.close()
            except Exception as exc:
                logger.warning(
                    "State store close failed: %s",
                    exc,
                )

            self._state_store = None

        self._aggregator = None
        self._initialized = False

        logger.info(
            "Worker resources closed: %s",
            self.worker_id,
        )


# ============================================================
# SIGNAL HANDLERS
# ============================================================


def install_signal_handlers(
    worker: StreamForgeWorker,
) -> None:
    """Install SIGINT and SIGTERM handlers."""

    def handle_signal(
        signum: int,
        _frame: Any,
    ) -> None:
        logger.info(
            "Received shutdown signal: %s",
            signum,
        )

        worker.stop()

    try:
        signal.signal(
            signal.SIGINT,
            handle_signal,
        )

        signal.signal(
            signal.SIGTERM,
            handle_signal,
        )

    except ValueError:
        logger.debug(
            "Signal handlers unavailable outside "
            "main thread."
        )


# ============================================================
# SELF TEST
# ============================================================


def run_self_test() -> None:
    """
    Run worker tests without connecting to Kafka.

    This validates the worker's pure Python functionality.
    """

    print("StreamForge worker self-test")
    print("-" * 60)

    temporary_directory = Path(
        tempfile.mkdtemp(
            prefix="streamforge-worker-test-"
        )
    )

    worker: Optional[StreamForgeWorker] = None

    try:
        # ----------------------------------------------------
        # Worker creation
        # ----------------------------------------------------

        worker = StreamForgeWorker(
            worker_id="self-test-worker",
            bootstrap_servers="127.0.0.1:9092",
            state_directory=(
                temporary_directory / "state"
            ),
        )

        print("Worker creation: OK")

        # ----------------------------------------------------
        # Health
        # ----------------------------------------------------

        health = worker.health()

        assert (
            health["worker_id"]
            == "self-test-worker"
        )

        assert health["initialized"] is False
        assert health["running"] is False

        print("Worker health: OK")

        # ----------------------------------------------------
        # Statistics
        # ----------------------------------------------------

        stats = worker.get_statistics()

        assert (
            stats["worker_id"]
            == "self-test-worker"
        )

        assert stats["messages_received"] == 0
        assert stats["messages_processed"] == 0

        print("Worker statistics: OK")

        # ----------------------------------------------------
        # Telemetry payload
        # ----------------------------------------------------

        payload = {
            "event_id": "self-test-event-001",
            "device_id": "device-001",
            "timestamp": time.time(),
            "temperature": 25.5,
            "humidity": 55.0,
            "pressure": 1013.25,
            "battery": 95.0,
            "sequence": 1,
        }

        # ----------------------------------------------------
        # Dictionary decoding
        # ----------------------------------------------------

        decoded = worker.decode_event(payload)

        assert decoded == payload

        print("Dictionary decoding: OK")

        # ----------------------------------------------------
        # JSON decoding
        # ----------------------------------------------------

        encoded = json.dumps(payload)

        decoded = worker.decode_event(encoded)

        assert decoded == payload

        print("JSON decoding: OK")

        # ----------------------------------------------------
        # Bytes decoding
        # ----------------------------------------------------

        decoded = worker.decode_event(
            encoded.encode("utf-8")
        )

        assert decoded == payload

        print("Bytes decoding: OK")

        # ----------------------------------------------------
        # Invalid JSON
        # ----------------------------------------------------

        failed = False

        try:
            worker.decode_event(
                "{invalid-json"
            )
        except TelemetryProcessingError:
            failed = True

        assert failed

        print("Invalid JSON handling: OK")

        # ----------------------------------------------------
        # Pydantic validation
        # ----------------------------------------------------

        event = worker.validate_event(payload)

        assert isinstance(
            event,
            TelemetryEvent,
        )

        assert event.device_id == "device-001"
        assert event.temperature == 25.5

        print("Telemetry validation: OK")

        # ----------------------------------------------------
        # Invalid telemetry
        # ----------------------------------------------------

        invalid_payload = {
            "event_id": "bad-event",
            "device_id": "device-001",
            "timestamp": time.time(),
            "temperature": "not-a-number",
            "humidity": 55.0,
            "pressure": 1013.25,
            "battery": 95.0,
            "sequence": 1,
        }

        failed = False

        try:
            worker.validate_event(
                invalid_payload
            )
        except TelemetryProcessingError:
            failed = True

        assert failed

        print("Invalid telemetry handling: OK")

        # ----------------------------------------------------
        # Aggregate key generation
        # ----------------------------------------------------

        aggregate_payload = {
            "device_id": "device-001",
            "window_start": 1770000000.0,
            "window_end": 1770000300.0,
            "count": 10,
            "average_temperature": 25.5,
        }

        aggregate_key = (
            worker.build_aggregate_key(
                aggregate_payload
            )
        )

        assert (
            aggregate_key
            == "device-001:1770000000.0"
        )

        print("Aggregate Kafka key generation: OK")

        # ----------------------------------------------------
        # Object conversion
        # ----------------------------------------------------

        event_dict = worker.object_to_dict(event)

        assert isinstance(event_dict, dict)
        assert event_dict["device_id"] == "device-001"

        print("Object-to-dictionary conversion: OK")

        # ----------------------------------------------------
        # Statistics calculation
        # ----------------------------------------------------

        worker._statistics.started_at = (
            time.time() - 10
        )

        worker._statistics.messages_received = 10
        worker._statistics.messages_processed = 8
        worker._statistics.messages_failed = 2
        worker._statistics.processing_time_seconds = 0.8

        stats = worker.get_statistics()

        assert stats["messages_received"] == 10
        assert stats["messages_processed"] == 8
        assert stats["messages_failed"] == 2
        assert stats["average_processing_ms"] > 0
        assert stats["messages_per_second"] > 0

        print("Statistics calculation: OK")

        # ----------------------------------------------------
        # Shutdown
        # ----------------------------------------------------

        worker.stop()

        assert worker._running is False

        print("Graceful shutdown: OK")

        # ----------------------------------------------------
        # Result
        # ----------------------------------------------------

        print("-" * 60)
        print("StreamForge worker self-test: OK")

    finally:
        if worker is not None:
            try:
                worker.close()
            except Exception:
                pass

        # StateStore/RocksDB needs time to release
        # Windows file handles before cleanup.
        time.sleep(0.3)

        try:
            import shutil

            shutil.rmtree(
                temporary_directory,
                ignore_errors=True,
            )
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    """Start the real StreamForge worker."""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    worker = StreamForgeWorker()

    install_signal_handlers(worker)

    try:
        worker.run()

    finally:
        worker.close()


if __name__ == "__main__":
    run_self_test()