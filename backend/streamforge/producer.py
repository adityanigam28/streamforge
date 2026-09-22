"""
StreamForge telemetry event producer.

Generates telemetry events and publishes them to Kafka.

Responsibilities:
- Generate realistic telemetry events
- Maintain per-device sequence numbers
- Validate events using TelemetryEvent
- Publish telemetry through the StreamForge Kafka abstraction
- Provide producer statistics and health information
- Support continuous and fixed-duration execution
- Provide a Kafka-independent self-test
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Lock
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from .config import settings
from .kafka import (
    KafkaIntegrationError,
    create_producer,
    publish_telemetry,
)
from .models import TelemetryEvent


logger = logging.getLogger(__name__)


# ============================================================================
# Exceptions
# ============================================================================


class ProducerError(Exception):
    """Base exception for producer errors."""


class ProducerConfigurationError(ProducerError):
    """Raised when producer configuration is invalid."""


class EventGenerationError(ProducerError):
    """Raised when event generation fails."""


# ============================================================================
# Statistics
# ============================================================================


@dataclass
class ProducerStats:
    """Runtime producer statistics."""

    events_generated: int = 0
    events_published: int = 0
    events_failed: int = 0
    batches_published: int = 0
    start_time: Optional[float] = None
    stop_time: Optional[float] = None

    @property
    def elapsed_seconds(self) -> float:
        """Return producer runtime."""

        if self.start_time is None:
            return 0.0

        end = (
            self.stop_time
            if self.stop_time is not None
            else time.monotonic()
        )

        return max(
            0.0,
            end - self.start_time,
        )

    @property
    def events_per_second(self) -> float:
        """Return observed publishing rate."""

        elapsed = self.elapsed_seconds

        if elapsed <= 0:
            return 0.0

        return self.events_published / elapsed

    def to_dict(self) -> Dict[str, Any]:
        """Return statistics as a dictionary."""

        return {
            "events_generated": self.events_generated,
            "events_published": self.events_published,
            "events_failed": self.events_failed,
            "batches_published": self.batches_published,
            "elapsed_seconds": round(
                self.elapsed_seconds,
                3,
            ),
            "events_per_second": round(
                self.events_per_second,
                3,
            ),
        }


# ============================================================================
# Telemetry Producer
# ============================================================================


class TelemetryProducer:
    """Generate and publish telemetry events."""

    def __init__(
        self,
        device_count: Optional[int] = None,
        events_per_second: Optional[float] = None,
        temperature_min: Optional[float] = None,
        temperature_max: Optional[float] = None,
        topic: Optional[str] = None,
        kafka_producer: Any = None,
        seed: Optional[int] = None,
    ) -> None:

        self.device_count = (
            settings.producer.device_count
            if device_count is None
            else device_count
        )

        self.events_per_second = (
            settings.producer.events_per_second
            if events_per_second is None
            else events_per_second
        )

        self.temperature_min = (
            settings.producer.temperature_min
            if temperature_min is None
            else temperature_min
        )

        self.temperature_max = (
            settings.producer.temperature_max
            if temperature_max is None
            else temperature_max
        )

        self.topic = (
            settings.kafka.telemetry_topic
            if topic is None
            else topic
        )

        self._producer = kafka_producer

        self._owns_producer = (
            kafka_producer is None
        )

        self._running = False
        self._stop_event = Event()

        self._stats = ProducerStats()

        self._sequence_numbers: Dict[str, int] = {}

        self._lock = Lock()

        self._random = random.Random(seed)

        self._validate_configuration()

    # ------------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------------

    def _validate_configuration(self) -> None:
        """Validate producer configuration."""

        if (
            not isinstance(
                self.device_count,
                int,
            )
            or self.device_count <= 0
        ):
            raise ProducerConfigurationError(
                "device_count must be a positive integer"
            )

        if (
            not isinstance(
                self.events_per_second,
                (int, float),
            )
            or self.events_per_second <= 0
        ):
            raise ProducerConfigurationError(
                "events_per_second must be greater than zero"
            )

        if not isinstance(
            self.temperature_min,
            (int, float),
        ):
            raise ProducerConfigurationError(
                "temperature_min must be numeric"
            )

        if not isinstance(
            self.temperature_max,
            (int, float),
        ):
            raise ProducerConfigurationError(
                "temperature_max must be numeric"
            )

        if self.temperature_min >= self.temperature_max:
            raise ProducerConfigurationError(
                "temperature_min must be less than temperature_max"
            )

        if not self.topic:
            raise ProducerConfigurationError(
                "Kafka telemetry topic must not be empty"
            )

    # ------------------------------------------------------------------------
    # Devices
    # ------------------------------------------------------------------------

    def device_ids(self) -> List[str]:
        """Return configured device IDs."""

        return [
            f"device-{index:04d}"
            for index in range(
                1,
                self.device_count + 1,
            )
        ]

    def _next_sequence(
        self,
        device_id: str,
    ) -> int:
        """Return the next sequence number for a device."""

        with self._lock:
            sequence = self._sequence_numbers.get(
                device_id,
                0,
            )

            self._sequence_numbers[device_id] = sequence + 1

            return sequence

    # ------------------------------------------------------------------------
    # Event generation
    # ------------------------------------------------------------------------

    def generate_event(
        self,
        device_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> TelemetryEvent:
        """Generate one validated telemetry event."""

        try:
            if device_id is None:
                device_id = self._random.choice(
                    self.device_ids()
                )

            if not device_id:
                raise EventGenerationError(
                    "device_id must not be empty"
                )

            if timestamp is None:
                timestamp = datetime.now(timezone.utc)

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(
                    tzinfo=timezone.utc
                )

            event = TelemetryEvent(
                event_id=str(uuid.uuid4()),
                device_id=device_id,
                timestamp=timestamp,
                temperature=round(
                    self._random.uniform(
                        self.temperature_min,
                        self.temperature_max,
                    ),
                    3,
                ),
                humidity=round(
                    self._random.uniform(
                        30.0,
                        90.0,
                    ),
                    3,
                ),
                pressure=round(
                    self._random.uniform(
                        980.0,
                        1040.0,
                    ),
                    3,
                ),
                battery=round(
                    self._random.uniform(
                        20.0,
                        100.0,
                    ),
                    3,
                ),
                sequence=self._next_sequence(
                    device_id
                ),
            )

            self._stats.events_generated += 1

            return event

        except ValidationError as exc:
            raise EventGenerationError(
                f"Generated event failed validation: {exc}"
            ) from exc

        except EventGenerationError:
            raise

        except Exception as exc:
            raise EventGenerationError(
                f"Failed to generate event: {exc}"
            ) from exc

    def generate_events(
        self,
        count: int,
    ) -> List[TelemetryEvent]:
        """Generate multiple telemetry events."""

        if (
            not isinstance(count, int)
            or count < 0
        ):
            raise ValueError(
                "count must be a non-negative integer"
            )

        return [
            self.generate_event()
            for _ in range(count)
        ]

    # ------------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------------

    @staticmethod
    def event_to_dict(
        event: TelemetryEvent,
    ) -> Dict[str, Any]:
        """Convert event to a JSON-compatible dictionary."""

        if not isinstance(
            event,
            TelemetryEvent,
        ):
            raise TypeError(
                "event must be a TelemetryEvent"
            )

        if hasattr(event, "model_dump"):
            return event.model_dump(
                mode="json"
            )

        return json.loads(
            event.json()
        )

    @staticmethod
    def event_to_json(
        event: TelemetryEvent,
    ) -> str:
        """Convert event to JSON."""

        return json.dumps(
            TelemetryProducer.event_to_dict(
                event
            ),
            separators=(",", ":"),
        )

    # ------------------------------------------------------------------------
    # Kafka lifecycle
    # ------------------------------------------------------------------------

    def connect(self) -> None:
        """Create the Kafka producer."""

        if self._producer is not None:
            return

        try:
            self._producer = create_producer()
            self._owns_producer = True

            logger.info(
                "Kafka producer connected"
            )

        except KafkaIntegrationError as exc:
            raise ProducerError(
                f"Unable to create Kafka producer: {exc}"
            ) from exc

        except Exception as exc:
            raise ProducerError(
                f"Unable to connect Kafka producer: {exc}"
            ) from exc

    def close(self) -> None:
        """Close the Kafka producer."""

        self._running = False
        self._stop_event.set()

        if (
            self._producer is not None
            and self._owns_producer
        ):
            try:
                self._producer.close()

            except Exception as exc:
                logger.warning(
                    "Error closing Kafka producer: %s",
                    exc,
                )

            finally:
                self._producer = None

        if self._stats.start_time is not None:
            self._stats.stop_time = time.monotonic()

    # ------------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------------

    def publish_event(
        self,
        event: TelemetryEvent,
    ) -> Any:
        """Publish one telemetry event."""

        if not isinstance(
            event,
            TelemetryEvent,
        ):
            raise TypeError(
                "event must be a TelemetryEvent"
            )

        self.connect()

        try:
            result = publish_telemetry(
                self._producer,
                event,
            )

            self._stats.events_published += 1

            return result

        except Exception as exc:
            self._stats.events_failed += 1

            raise ProducerError(
                f"Failed to publish event "
                f"{event.event_id}: {exc}"
            ) from exc

    def publish_events(
        self,
        events: List[TelemetryEvent],
    ) -> int:
        """Publish a list of telemetry events."""

        if not isinstance(
            events,
            list,
        ):
            raise TypeError(
                "events must be a list"
            )

        if not events:
            return 0

        for event in events:
            self.publish_event(event)

        self._stats.batches_published += 1

        return len(events)

    # ------------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------------

    def start(self) -> None:
        """Run continuously until stopped."""

        if self._running:
            return

        self.connect()

        self._running = True
        self._stop_event.clear()

        self._stats.start_time = time.monotonic()
        self._stats.stop_time = None

        interval = (
            1.0 / float(self.events_per_second)
        )

        logger.info(
            "Starting producer: devices=%d, "
            "rate=%.2f events/sec",
            self.device_count,
            self.events_per_second,
        )

        try:
            while not self._stop_event.is_set():

                started = time.monotonic()

                event = self.generate_event()

                self.publish_event(event)

                elapsed = (
                    time.monotonic()
                    - started
                )

                delay = max(
                    0.0,
                    interval - elapsed,
                )

                if delay > 0:
                    self._stop_event.wait(delay)

        finally:
            self._running = False
            self._stats.stop_time = time.monotonic()

    def run_for(
        self,
        duration_seconds: float,
    ) -> ProducerStats:
        """Run the producer for a fixed duration."""

        if duration_seconds <= 0:
            raise ValueError(
                "duration_seconds must be greater than zero"
            )

        if self._running:
            raise ProducerError(
                "Producer is already running"
            )

        self.connect()

        self._running = True
        self._stop_event.clear()

        self._stats.start_time = time.monotonic()
        self._stats.stop_time = None

        interval = (
            1.0 / float(self.events_per_second)
        )

        deadline = (
            time.monotonic()
            + duration_seconds
        )

        try:
            while not self._stop_event.is_set():

                if time.monotonic() >= deadline:
                    break

                started = time.monotonic()

                event = self.generate_event()

                self.publish_event(event)

                elapsed = (
                    time.monotonic()
                    - started
                )

                delay = max(
                    0.0,
                    interval - elapsed,
                )

                remaining = (
                    deadline
                    - time.monotonic()
                )

                if remaining <= 0:
                    break

                self._stop_event.wait(
                    min(
                        delay,
                        remaining,
                    )
                )

        finally:
            self._running = False
            self._stats.stop_time = time.monotonic()

        return self.stats()

    def stop(self) -> None:
        """Request producer shutdown."""

        self._stop_event.set()
        self._running = False

    # ------------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------------

    def is_running(self) -> bool:
        """Return whether producer is running."""

        return self._running

    def stats(self) -> ProducerStats:
        """Return producer statistics."""

        return ProducerStats(
            events_generated=self._stats.events_generated,
            events_published=self._stats.events_published,
            events_failed=self._stats.events_failed,
            batches_published=self._stats.batches_published,
            start_time=self._stats.start_time,
            stop_time=self._stats.stop_time,
        )

    def health(self) -> Dict[str, Any]:
        """Return producer health."""

        return {
            "status": (
                "running"
                if self._running
                else "stopped"
            ),
            "topic": self.topic,
            "device_count": self.device_count,
            "events_per_second": self.events_per_second,
            "kafka_connected": (
                self._producer is not None
            ),
            "statistics": self.stats().to_dict(),
        }


# ============================================================================
# Signal handling
# ============================================================================


def _install_signal_handlers(
    producer: TelemetryProducer,
) -> None:
    """Install graceful shutdown handlers."""

    def handle_signal(
        signum: int,
        _frame: Any,
    ) -> None:

        logger.info(
            "Received signal %s",
            signum,
        )

        producer.stop()

    signal.signal(
        signal.SIGINT,
        handle_signal,
    )

    if hasattr(signal, "SIGTERM"):
        signal.signal(
            signal.SIGTERM,
            handle_signal,
        )


# ============================================================================
# CLI
# ============================================================================


def build_argument_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""

    parser = argparse.ArgumentParser(
        description="StreamForge telemetry producer"
    )

    parser.add_argument(
        "--devices",
        type=int,
        default=None,
        help="Number of simulated devices",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Events per second",
    )

    parser.add_argument(
        "--temperature-min",
        type=float,
        default=None,
        help="Minimum temperature",
    )

    parser.add_argument(
        "--temperature-max",
        type=float,
        default=None,
        help="Maximum temperature",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Run duration in seconds",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=[
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        ],
        help="Logging level",
    )

    return parser


def main() -> None:
    """CLI entry point."""

    parser = build_argument_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(
            logging,
            args.log_level,
        ),
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    producer = TelemetryProducer(
        device_count=args.devices,
        events_per_second=args.rate,
        temperature_min=args.temperature_min,
        temperature_max=args.temperature_max,
        seed=args.seed,
    )

    _install_signal_handlers(producer)

    try:
        if args.duration is not None:
            producer.run_for(args.duration)
        else:
            producer.start()

    except KeyboardInterrupt:
        logger.info(
            "Producer interrupted"
        )

    finally:
        logger.info(
            "Producer statistics: %s",
            producer.stats().to_dict(),
        )

        producer.close()


# ============================================================================
# Self-test fake Kafka implementation
# ============================================================================


class _FakeKafkaMetadata:
    """Fake Kafka metadata object."""

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:

        self.topic = topic
        self.partition = partition
        self.offset = offset


class _FakeKafkaFuture:
    """Fake Kafka future."""

    def __init__(
        self,
        metadata: _FakeKafkaMetadata,
    ) -> None:

        self.metadata = metadata

    def get(
        self,
        timeout: Optional[float] = None,
    ) -> _FakeKafkaMetadata:
        """Return fake Kafka metadata."""

        return self.metadata


class _FakeKafkaProducer:
    """Fake Kafka producer for unit testing."""

    def __init__(self) -> None:

        self.messages: List[
            Dict[str, Any]
        ] = []

        self.closed = False

    def send(
        self,
        topic: str,
        key: Optional[bytes] = None,
        value: Optional[bytes] = None,
        **kwargs: Any,
    ) -> _FakeKafkaFuture:

        offset = len(self.messages)

        self.messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "kwargs": kwargs,
            }
        )

        metadata = _FakeKafkaMetadata(
            topic=topic,
            partition=0,
            offset=offset,
        )

        return _FakeKafkaFuture(metadata)

    def flush(
        self,
        timeout: Optional[float] = None,
    ) -> None:
        """Fake flush operation."""

    def close(self) -> None:
        """Fake close operation."""

        self.closed = True


# ============================================================================
# Self-test
# ============================================================================


def _decode_fake_kafka_payload(
    value: Any,
    producer: TelemetryProducer,
) -> Dict[str, Any]:
    """
    Decode a fake Kafka payload.

    The real kafka.py abstraction owns serialization.
    The self-test therefore accepts multiple representations.
    """

    if isinstance(value, (bytes, bytearray)):
        return json.loads(
            bytes(value).decode("utf-8")
        )

    if isinstance(value, str):
        return json.loads(value)

    if isinstance(value, TelemetryEvent):
        return producer.event_to_dict(value)

    if hasattr(value, "model_dump"):
        return value.model_dump(
            mode="json"
        )

    if isinstance(value, dict):
        return value

    raise AssertionError(
        "Unsupported fake Kafka payload type: "
        f"{type(value).__name__}"
    )


def _run_self_test() -> None:
    """Run producer self-test."""

    print("StreamForge producer self-test")
    print("-" * 60)

    # ------------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------------

    producer = TelemetryProducer(
        device_count=3,
        events_per_second=10,
        temperature_min=10.0,
        temperature_max=40.0,
        topic="telemetry-test",
        seed=42,
    )

    assert producer.device_count == 3
    assert producer.events_per_second == 10
    assert producer.temperature_min == 10.0
    assert producer.temperature_max == 40.0
    assert producer.topic == "telemetry-test"

    print("Producer configuration: OK")

    # ------------------------------------------------------------------------
    # Device IDs
    # ------------------------------------------------------------------------

    assert producer.device_ids() == [
        "device-0001",
        "device-0002",
        "device-0003",
    ]

    print("Device ID generation: OK")

    # ------------------------------------------------------------------------
    # Event generation
    # ------------------------------------------------------------------------

    event = producer.generate_event(
        device_id="device-0001",
        timestamp=datetime(
            2026,
            1,
            1,
            tzinfo=timezone.utc,
        ),
    )

    assert isinstance(
        event,
        TelemetryEvent,
    )

    assert event.device_id == "device-0001"
    assert event.sequence == 0

    assert (
        10.0
        <= event.temperature
        <= 40.0
    )

    assert (
        30.0
        <= event.humidity
        <= 90.0
    )

    assert (
        980.0
        <= event.pressure
        <= 1040.0
    )

    assert (
        20.0
        <= event.battery
        <= 100.0
    )

    print("Telemetry event generation: OK")

    # ------------------------------------------------------------------------
    # Sequence numbers
    # ------------------------------------------------------------------------

    event2 = producer.generate_event(
        device_id="device-0001"
    )

    event3 = producer.generate_event(
        device_id="device-0001"
    )

    assert event2.sequence == 1
    assert event3.sequence == 2

    print("Per-device sequence numbers: OK")

    # ------------------------------------------------------------------------
    # Independent device sequence
    # ------------------------------------------------------------------------

    other = producer.generate_event(
        device_id="device-0002"
    )

    assert other.sequence == 0

    print("Independent device sequences: OK")

    # ------------------------------------------------------------------------
    # Batch generation
    # ------------------------------------------------------------------------

    batch = producer.generate_events(5)

    assert len(batch) == 5

    assert all(
        isinstance(
            item,
            TelemetryEvent,
        )
        for item in batch
    )

    print("Batch event generation: OK")

    # ------------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------------

    event_dict = producer.event_to_dict(event)

    assert isinstance(
        event_dict,
        dict,
    )

    assert (
        event_dict["event_id"]
        == event.event_id
    )

    assert (
        event_dict["device_id"]
        == event.device_id
    )

    assert "timestamp" in event_dict

    event_json = producer.event_to_json(event)

    assert isinstance(
        event_json,
        str,
    )

    decoded = json.loads(event_json)

    assert (
        decoded["event_id"]
        == event.event_id
    )

    assert (
        decoded["device_id"]
        == event.device_id
    )

    print("Event serialization: OK")

    # ------------------------------------------------------------------------
    # Kafka publishing
    # ------------------------------------------------------------------------

    fake_kafka = _FakeKafkaProducer()

    kafka_producer = TelemetryProducer(
        device_count=1,
        events_per_second=1,
        temperature_min=0.0,
        temperature_max=50.0,
        topic="telemetry-test",
        kafka_producer=fake_kafka,
        seed=1,
    )

    published_event = kafka_producer.generate_event(
        device_id="device-0001"
    )

    # kafka.py owns the actual topic,
    # key serialization, payload serialization,
    # and return type.
    kafka_producer.publish_event(
        published_event
    )

    assert len(
        fake_kafka.messages
    ) == 1

    message = fake_kafka.messages[0]

    # The Kafka abstraction determines the
    # configured telemetry topic.
    assert message["topic"] == (
        settings.kafka.telemetry_topic
    )

    # A telemetry event must have a key.
    assert message["key"] is not None

    # Payload must exist.
    assert message["value"] is not None

    # FIX:
    # The fake Kafka payload may be bytes,
    # string, TelemetryEvent, Pydantic model,
    # or dictionary depending on kafka.py.
    decoded_message = _decode_fake_kafka_payload(
        message["value"],
        kafka_producer,
    )

    assert (
        decoded_message["device_id"]
        == "device-0001"
    )

    assert (
        decoded_message["event_id"]
        == published_event.event_id
    )

    print("Kafka event publishing: OK")

    # ------------------------------------------------------------------------
    # Batch publishing
    # ------------------------------------------------------------------------

    batch_events = (
        kafka_producer.generate_events(3)
    )

    count = kafka_producer.publish_events(
        batch_events
    )

    assert count == 3

    assert len(
        fake_kafka.messages
    ) == 4

    print("Batch Kafka publishing: OK")

    # ------------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------------

    stats = kafka_producer.stats()

    assert stats.events_generated == 4
    assert stats.events_published == 4
    assert stats.events_failed == 0
    assert stats.batches_published == 1

    print("Producer statistics: OK")

    # ------------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------------

    health = kafka_producer.health()

    assert health["status"] == "stopped"

    assert health["topic"] == "telemetry-test"

    assert health["device_count"] == 1

    assert health["kafka_connected"] is True

    print("Producer health: OK")

    # ------------------------------------------------------------------------
    # Invalid configuration
    # ------------------------------------------------------------------------

    try:
        TelemetryProducer(
            device_count=0,
            topic="telemetry-test",
        )
        raise AssertionError

    except ProducerConfigurationError:
        pass

    try:
        TelemetryProducer(
            events_per_second=0,
            topic="telemetry-test",
        )
        raise AssertionError

    except ProducerConfigurationError:
        pass

    try:
        TelemetryProducer(
            temperature_min=50.0,
            temperature_max=10.0,
            topic="telemetry-test",
        )
        raise AssertionError

    except ProducerConfigurationError:
        pass

    print("Invalid configuration handling: OK")

    # ------------------------------------------------------------------------
    # Invalid batch
    # ------------------------------------------------------------------------

    try:
        producer.generate_events(-1)
        raise AssertionError

    except ValueError:
        pass

    print("Invalid batch handling: OK")

    # ------------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------------

    kafka_producer.close()

    assert fake_kafka.closed is True

    print("Graceful shutdown: OK")

    print("-" * 60)
    print("StreamForge producer self-test: OK")


# ============================================================================
# Entry point
# ============================================================================


if __name__ == "__main__":
    _run_self_test()