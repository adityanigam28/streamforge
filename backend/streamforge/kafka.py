"""
StreamForge Kafka Integration
=============================

Kafka infrastructure used by StreamForge.

Responsibilities:
    - Create Kafka producers
    - Create Kafka consumers
    - Serialize and deserialize JSON payloads
    - Publish telemetry and aggregate records
    - Publish worker metrics
    - Provide Kafka health checks
    - Provide topic metadata helpers
    - Provide controlled offset commits
    - Provide a lightweight self-test

Kafka is the transport layer between the StreamForge components.

Pipeline:

    Producer
       |
       v
    telemetry
       |
       v
    Worker
       |
       v
    temperature-aggregates

State/changelog and worker-metrics topics are also supported.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.structs import TopicPartition


from .config import settings


logger = logging.getLogger(__name__)


# ============================================================
# DEFAULTS
# ============================================================

DEFAULT_REQUEST_TIMEOUT_MS = 30000
DEFAULT_SESSION_TIMEOUT_MS = 10000
DEFAULT_AUTO_OFFSET_RESET = "earliest"
DEFAULT_MAX_POLL_RECORDS = 500

DEFAULT_ACKS = "all"
DEFAULT_RETRIES = 5
DEFAULT_LINGER_MS = 5


# ============================================================
# EXCEPTIONS
# ============================================================


class KafkaIntegrationError(Exception):
    """Base exception for StreamForge Kafka errors."""


class KafkaConfigurationError(KafkaIntegrationError):
    """Raised when Kafka configuration is invalid."""


class KafkaPublishError(KafkaIntegrationError):
    """Raised when publishing a Kafka message fails."""


class KafkaConnectionError(KafkaIntegrationError):
    """Raised when Kafka cannot be reached."""


# ============================================================
# DATA STRUCTURES
# ============================================================


@dataclass(frozen=True)
class KafkaMessage:
    """Simple representation of a consumed Kafka message."""

    topic: str
    partition: int
    offset: int
    key: Any
    value: Any
    timestamp: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return the message as a dictionary."""

        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
        }


# ============================================================
# SERIALIZATION
# ============================================================


def serialize_json(value: Any) -> bytes:
    """
    Serialize a Python value to compact UTF-8 JSON.

    Pydantic models and other objects supporting model_dump()
    are converted before JSON serialization.
    """

    if hasattr(value, "model_dump"):
        value = value.model_dump()

    elif hasattr(value, "dict"):
        value = value.dict()

    return json.dumps(
        value,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def deserialize_json(value: Optional[bytes]) -> Any:
    """
    Deserialize UTF-8 JSON.

    None remains None.
    """

    if value is None:
        return None

    if isinstance(value, str):
        return json.loads(value)

    if isinstance(value, bytes):
        return json.loads(
            value.decode("utf-8")
        )

    raise KafkaIntegrationError(
        "Kafka JSON payload must be bytes, str, or None."
    )


def serialize_key(key: Any) -> Optional[bytes]:
    """Serialize a Kafka key."""

    if key is None:
        return None

    if isinstance(key, bytes):
        return key

    return str(key).encode("utf-8")


def deserialize_key(key: Optional[bytes]) -> Optional[str]:
    """Deserialize a Kafka key to a string."""

    if key is None:
        return None

    if isinstance(key, str):
        return key

    return key.decode("utf-8")


# ============================================================
# CONFIGURATION
# ============================================================


def get_bootstrap_servers() -> str:
    """Return configured Kafka bootstrap servers."""

    servers = settings.kafka.bootstrap_servers

    if not servers:
        raise KafkaConfigurationError(
            "Kafka bootstrap servers are not configured."
        )

    return servers


def get_kafka_topics() -> Dict[str, str]:
    """Return all StreamForge Kafka topics."""

    return {
        "telemetry": (
            settings.kafka.telemetry_topic
        ),
        "state_changelog": (
            settings.kafka.state_changelog_topic
        ),
        "temperature_aggregates": (
            settings.kafka.temperature_aggregates_topic
        ),
        "worker_metrics": (
            settings.kafka.worker_metrics_topic
        ),
    }


def validate_kafka_configuration() -> None:
    """Validate the Kafka configuration."""

    bootstrap_servers = (
        settings.kafka.bootstrap_servers
    )

    if not bootstrap_servers:
        raise KafkaConfigurationError(
            "Kafka bootstrap servers cannot be empty."
        )

    topics = get_kafka_topics()

    for name, topic in topics.items():
        if not topic:
            raise KafkaConfigurationError(
                f"Kafka topic '{name}' cannot be empty."
            )

    if not settings.kafka.consumer_group:
        raise KafkaConfigurationError(
            "Kafka consumer group cannot be empty."
        )


# ============================================================
# PRODUCER
# ============================================================


def create_producer(
    client_id: str = "streamforge-producer",
) -> KafkaProducer:
    """
    Create a configured Kafka producer.

    Delivery semantics:
        acks='all'
        retries=5

    This gives the application reliable producer behaviour
    without pretending to provide exactly-once semantics.
    """

    validate_kafka_configuration()

    try:
        producer = KafkaProducer(
            bootstrap_servers=(
                get_bootstrap_servers()
            ),
            client_id=client_id,
            acks=DEFAULT_ACKS,
            retries=DEFAULT_RETRIES,
            linger_ms=DEFAULT_LINGER_MS,
            key_serializer=serialize_key,
            value_serializer=serialize_json,
            request_timeout_ms=(
                DEFAULT_REQUEST_TIMEOUT_MS
            ),
        )

        logger.info(
            "Kafka producer created: client_id=%s",
            client_id,
        )

        return producer

    except NoBrokersAvailable as exc:
        raise KafkaConnectionError(
            "No Kafka broker is available at "
            f"{get_bootstrap_servers()}."
        ) from exc

    except KafkaError as exc:
        raise KafkaConnectionError(
            f"Failed to create Kafka producer: {exc}"
        ) from exc


# ============================================================
# CONSUMER
# ============================================================


def create_consumer(
    topic: str,
    group_id: Optional[str] = None,
    client_id: str = "streamforge-consumer",
    auto_commit: bool = False,
    auto_offset_reset: str = DEFAULT_AUTO_OFFSET_RESET,
    max_poll_records: Optional[int] = None,
) -> KafkaConsumer:
    """
    Create a configured Kafka consumer.

    Auto-commit defaults to False.

    StreamForge workers should commit offsets only after
    successful event processing.
    """

    validate_kafka_configuration()

    if not topic:
        raise KafkaConfigurationError(
            "Kafka consumer topic cannot be empty."
        )

    resolved_group_id = (
        group_id
        or settings.kafka.consumer_group
    )

    if not resolved_group_id:
        raise KafkaConfigurationError(
            "Kafka consumer group cannot be empty."
        )

    if auto_offset_reset not in {
        "earliest",
        "latest",
        "none",
    }:
        raise KafkaConfigurationError(
            "auto_offset_reset must be "
            "'earliest', 'latest', or 'none'."
        )

    resolved_max_poll_records = (
        max_poll_records
        if max_poll_records is not None
        else settings.worker.max_poll_records
    )

    if resolved_max_poll_records < 1:
        raise KafkaConfigurationError(
            "max_poll_records must be at least 1."
        )

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=(
                get_bootstrap_servers()
            ),
            group_id=resolved_group_id,
            client_id=client_id,
            auto_offset_reset=auto_offset_reset,

            # IMPORTANT:
            # StreamForge controls commits explicitly.
            enable_auto_commit=auto_commit,

            max_poll_records=(
                resolved_max_poll_records
            ),

            key_deserializer=deserialize_key,
            value_deserializer=deserialize_json,

            request_timeout_ms=(
                DEFAULT_REQUEST_TIMEOUT_MS
            ),
            session_timeout_ms=(
                DEFAULT_SESSION_TIMEOUT_MS
            ),
        )

        logger.info(
            "Kafka consumer created: "
            "topic=%s group=%s client_id=%s "
            "auto_commit=%s",
            topic,
            resolved_group_id,
            client_id,
            auto_commit,
        )

        return consumer

    except NoBrokersAvailable as exc:
        raise KafkaConnectionError(
            "No Kafka broker is available at "
            f"{get_bootstrap_servers()}."
        ) from exc

    except KafkaError as exc:
        raise KafkaConnectionError(
            f"Failed to create Kafka consumer: {exc}"
        ) from exc


# ============================================================
# PUBLISH
# ============================================================


def publish(
    producer: KafkaProducer,
    topic: str,
    value: Any,
    key: Any = None,
    timeout: float = 10.0,
) -> Any:
    """
    Publish one message and wait for broker acknowledgement.

    Returns Kafka RecordMetadata.
    """

    if producer is None:
        raise KafkaPublishError(
            "Kafka producer cannot be None."
        )

    if not topic:
        raise KafkaPublishError(
            "Kafka topic cannot be empty."
        )

    try:
        future = producer.send(
            topic,
            key=key,
            value=value,
        )

        metadata = future.get(
            timeout=timeout
        )

        logger.debug(
            "Kafka message published: "
            "topic=%s partition=%s offset=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

        return metadata

    except Exception as exc:
        raise KafkaPublishError(
            f"Failed to publish Kafka message "
            f"to topic '{topic}': {exc}"
        ) from exc


def publish_telemetry(
    producer: KafkaProducer,
    event: Any,
    device_id: Optional[str] = None,
) -> Any:
    """Publish a telemetry event."""

    if device_id is None:
        if hasattr(event, "device_id"):
            device_id = event.device_id

        elif isinstance(event, dict):
            device_id = event.get("device_id")

    return publish(
        producer=producer,
        topic=settings.kafka.telemetry_topic,
        key=device_id,
        value=event,
    )


def publish_aggregate(
    producer: KafkaProducer,
    aggregate: Any,
    key: Optional[str] = None,
) -> Any:
    """
    Publish an aggregate result.

    The aggregate topic is compacted, so a stable key should
    normally be supplied.
    """

    if key is None:
        if hasattr(aggregate, "device_id"):
            device_id = aggregate.device_id
            window_start = getattr(
                aggregate,
                "window_start",
                None,
            )

        elif isinstance(aggregate, dict):
            device_id = aggregate.get(
                "device_id"
            )
            window_start = aggregate.get(
                "window_start"
            )

        else:
            device_id = None
            window_start = None

        if (
            device_id is not None
            and window_start is not None
        ):
            key = (
                f"{device_id}:{window_start}"
            )

    return publish(
        producer=producer,
        topic=(
            settings.kafka
            .temperature_aggregates_topic
        ),
        key=key,
        value=aggregate,
    )


def publish_worker_metrics(
    producer: KafkaProducer,
    worker_id: str,
    metrics: Dict[str, Any],
) -> Any:
    """Publish worker metrics."""

    payload = {
        "worker_id": worker_id,
        "timestamp": time.time(),
        "metrics": metrics,
    }

    return publish(
        producer=producer,
        topic=(
            settings.kafka.worker_metrics_topic
        ),
        key=worker_id,
        value=payload,
    )


# ============================================================
# CONSUMPTION
# ============================================================


def poll(
    consumer: KafkaConsumer,
    timeout_ms: int = 1000,
    max_records: Optional[int] = None,
) -> Dict[TopicPartition, List[Any]]:
    """Poll records from Kafka."""

    if consumer is None:
        raise KafkaIntegrationError(
            "Kafka consumer cannot be None."
        )

    kwargs: Dict[str, Any] = {
        "timeout_ms": timeout_ms,
    }

    if max_records is not None:
        kwargs["max_records"] = max_records

    try:
        return consumer.poll(**kwargs)

    except KafkaError as exc:
        raise KafkaIntegrationError(
            f"Kafka polling failed: {exc}"
        ) from exc


def commit(
    consumer: KafkaConsumer,
) -> None:
    """Commit the consumer's current offsets."""

    if consumer is None:
        raise KafkaIntegrationError(
            "Kafka consumer cannot be None."
        )

    try:
        consumer.commit()

        logger.debug(
            "Kafka consumer offsets committed."
        )

    except KafkaError as exc:
        raise KafkaIntegrationError(
            f"Kafka offset commit failed: {exc}"
        ) from exc


# ============================================================
# TOPIC / BROKER INFORMATION
# ============================================================


def create_admin_client(
    client_id: str = "streamforge-admin",
) -> KafkaAdminClient:
    """Create a Kafka administration client."""

    validate_kafka_configuration()

    try:
        return KafkaAdminClient(
            bootstrap_servers=(
                get_bootstrap_servers()
            ),
            client_id=client_id,
            request_timeout_ms=(
                DEFAULT_REQUEST_TIMEOUT_MS
            ),
        )

    except NoBrokersAvailable as exc:
        raise KafkaConnectionError(
            "No Kafka broker is available at "
            f"{get_bootstrap_servers()}."
        ) from exc

    except KafkaError as exc:
        raise KafkaConnectionError(
            f"Failed to create Kafka admin client: {exc}"
        ) from exc


def list_topics(
    admin_client: KafkaAdminClient,
) -> List[str]:
    """Return Kafka topics visible to the broker."""

    if admin_client is None:
        raise KafkaIntegrationError(
            "Kafka admin client cannot be None."
        )

    try:
        topics = admin_client.list_topics()

        return sorted(
            str(topic)
            for topic in topics
        )

    except KafkaError as exc:
        raise KafkaIntegrationError(
            f"Failed to list Kafka topics: {exc}"
        ) from exc


def topic_exists(
    admin_client: KafkaAdminClient,
    topic: str,
) -> bool:
    """Return True when a Kafka topic exists."""

    if not topic:
        return False

    return topic in set(
        list_topics(admin_client)
    )


def get_topic_partitions(
    admin_client: KafkaAdminClient,
    topic: str,
) -> List[int]:
    """Return partition IDs for a Kafka topic."""

    if not topic:
        raise KafkaConfigurationError(
            "Topic cannot be empty."
        )

    try:
        metadata = (
            admin_client.describe_topics(
                [topic]
            )
        )

        if not metadata:
            return []

        partitions = metadata[0].get(
            "partitions",
            [],
        )

        result: List[int] = []

        for partition in partitions:
            if isinstance(
                partition,
                dict,
            ):
                partition_id = partition.get(
                    "partition"
                )

                if partition_id is not None:
                    result.append(
                        int(partition_id)
                    )

        return sorted(result)

    except Exception as exc:
        raise KafkaIntegrationError(
            f"Failed to inspect topic '{topic}': {exc}"
        ) from exc


# ============================================================
# HEALTH CHECK
# ============================================================


def check_connection(
    timeout: float = 5.0,
) -> bool:
    """
    Check whether Kafka is reachable.

    This creates a temporary producer and requests broker
    metadata.
    """

    producer: Optional[KafkaProducer] = None

    try:
        producer = KafkaProducer(
            bootstrap_servers=(
                get_bootstrap_servers()
            ),
            client_id=(
                "streamforge-health-check"
            ),
            request_timeout_ms=(
                int(timeout * 1000)
            ),
        )

        # Request metadata from the broker.
        producer.bootstrap_connected()

        return True

    except Exception as exc:
        logger.warning(
            "Kafka health check failed: %s",
            exc,
        )

        return False

    finally:
        if producer is not None:
            try:
                producer.close(
                    timeout=2
                )
            except Exception:
                pass


def get_connection_info() -> Dict[str, Any]:
    """Return Kafka connection information."""

    return {
        "bootstrap_servers": (
            settings.kafka.bootstrap_servers
        ),
        "consumer_group": (
            settings.kafka.consumer_group
        ),
        "topics": get_kafka_topics(),
    }


# ============================================================
# RESOURCE HELPERS
# ============================================================


def close_producer(
    producer: Optional[KafkaProducer],
) -> None:
    """Safely close a Kafka producer."""

    if producer is None:
        return

    try:
        producer.flush(
            timeout=10
        )
    except Exception as exc:
        logger.warning(
            "Kafka producer flush failed: %s",
            exc,
        )

    try:
        producer.close(
            timeout=10
        )
    except Exception as exc:
        logger.warning(
            "Kafka producer close failed: %s",
            exc,
        )


def close_consumer(
    consumer: Optional[KafkaConsumer],
) -> None:
    """Safely close a Kafka consumer."""

    if consumer is None:
        return

    try:
        consumer.close()
    except Exception as exc:
        logger.warning(
            "Kafka consumer close failed: %s",
            exc,
        )


def close_admin_client(
    admin_client: Optional[KafkaAdminClient],
) -> None:
    """Safely close a Kafka admin client."""

    if admin_client is None:
        return

    try:
        admin_client.close()
    except Exception as exc:
        logger.warning(
            "Kafka admin client close failed: %s",
            exc,
        )


# ============================================================
# SELF TEST
# ============================================================


def run_self_test() -> None:
    """
    Run Kafka module tests.

    The first part is completely local and does not require
    Kafka.

    If Kafka is running, the test also verifies the real broker
    connection and topic visibility.
    """

    print("StreamForge Kafka self-test")
    print("-" * 60)

    # --------------------------------------------------------
    # Configuration
    # --------------------------------------------------------

    validate_kafka_configuration()

    print("Kafka configuration: OK")

    # --------------------------------------------------------
    # Serialization
    # --------------------------------------------------------

    payload = {
        "event_id": "self-test-event",
        "device_id": "device-001",
        "temperature": 25.5,
        "humidity": 50.0,
        "pressure": 1013.25,
        "battery": 95.0,
        "sequence": 1,
    }

    encoded = serialize_json(payload)

    assert isinstance(
        encoded,
        bytes,
    )

    decoded = deserialize_json(encoded)

    assert decoded == payload

    print("JSON serialization: OK")

    # --------------------------------------------------------
    # Key serialization
    # --------------------------------------------------------

    key = serialize_key(
        "device-001"
    )

    assert key == b"device-001"

    decoded_key = deserialize_key(key)

    assert decoded_key == "device-001"

    print("Kafka key serialization: OK")

    # --------------------------------------------------------
    # None handling
    # --------------------------------------------------------

    assert serialize_key(None) is None
    assert deserialize_key(None) is None
    assert deserialize_json(None) is None

    print("Null payload/key handling: OK")

    # --------------------------------------------------------
    # Topic configuration
    # --------------------------------------------------------

    topics = get_kafka_topics()

    assert "telemetry" in topics
    assert "state_changelog" in topics
    assert "temperature_aggregates" in topics
    assert "worker_metrics" in topics

    for topic in topics.values():
        assert topic

    print("Topic configuration: OK")

    # --------------------------------------------------------
    # Connection information
    # --------------------------------------------------------

    connection_info = get_connection_info()

    assert (
        connection_info["bootstrap_servers"]
        == settings.kafka.bootstrap_servers
    )

    assert (
        connection_info["consumer_group"]
        == settings.kafka.consumer_group
    )

    print("Connection configuration: OK")

    # --------------------------------------------------------
    # Real Kafka connection
    # --------------------------------------------------------

    kafka_available = check_connection()

    if kafka_available:
        print("Kafka broker connection: OK")

        admin_client: Optional[
            KafkaAdminClient
        ] = None

        try:
            admin_client = create_admin_client()

            available_topics = list_topics(
                admin_client
            )

            print(
                "Kafka topic discovery: OK"
            )

            # Verify required topics where possible.
            required_topics = list(
                topics.values()
            )

            missing_topics = [
                topic
                for topic in required_topics
                if topic not in available_topics
            ]

            if missing_topics:
                print(
                    "Kafka required-topic check: "
                    f"WARNING - missing {missing_topics}"
                )
            else:
                print(
                    "Kafka required-topic check: OK"
                )

        finally:
            close_admin_client(
                admin_client
            )

    else:
        print(
            "Kafka broker connection: SKIPPED "
            "(broker unavailable)"
        )

    # --------------------------------------------------------
    # Result
    # --------------------------------------------------------

    print("-" * 60)
    print("StreamForge Kafka self-test: OK")


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    """Run the Kafka self-test."""

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