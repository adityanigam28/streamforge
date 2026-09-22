"""
StreamForge Configuration
=========================

Central configuration for the StreamForge streaming system.

This module contains:
    - Kafka configuration
    - Topic names
    - Worker configuration
    - Aggregation configuration
    - State/recovery configuration
    - Metrics configuration
    - API configuration
    - Producer configuration

All other StreamForge modules should import configuration
from this module instead of hard-coding values.

Example:
    from streamforge.config import settings

    print(settings.kafka.bootstrap_servers)
    print(settings.kafka.telemetry_topic)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

# backend/streamforge/config.py
#
# parents[0] = streamforge
# parents[1] = backend
# parents[2] = project root

PACKAGE_DIR = Path(__file__).resolve().parent
BACKEND_DIR = PACKAGE_DIR.parent
PROJECT_DIR = BACKEND_DIR.parent

DATA_DIR = PROJECT_DIR / "data"
STATE_DIR = DATA_DIR / "state"
LOG_DIR = DATA_DIR / "logs"

# Create required directories automatically.
DATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

STATE_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================
# ENVIRONMENT HELPERS
# ============================================================


def _get_env(
    name: str,
    default: str,
) -> str:
    """
    Read a string environment variable.
    """

    value = os.getenv(name)

    if value is None:
        return default

    value = value.strip()

    if not value:
        return default

    return value


def _get_int(
    name: str,
    default: int,
) -> int:
    """
    Read an integer environment variable safely.
    """

    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(value)

    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name!r} "
            f"must be an integer. "
            f"Received: {value!r}"
        ) from exc


def _get_float(
    name: str,
    default: float,
) -> float:
    """
    Read a floating-point environment variable safely.
    """

    value = os.getenv(name)

    if value is None:
        return default

    try:
        return float(value)

    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name!r} "
            f"must be a number. "
            f"Received: {value!r}"
        ) from exc


def _get_bool(
    name: str,
    default: bool,
) -> bool:
    """
    Read a boolean environment variable safely.

    Accepted true values:
        1
        true
        yes
        on

    Accepted false values:
        0
        false
        no
        off
    """

    value = os.getenv(name)

    if value is None:
        return default

    normalized = value.strip().lower()

    if normalized in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return True

    if normalized in {
        "0",
        "false",
        "no",
        "off",
    }:
        return False

    raise ValueError(
        f"Environment variable {name!r} "
        f"must be a boolean. "
        f"Received: {value!r}"
    )


# ============================================================
# KAFKA CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class KafkaSettings:
    """
    Kafka configuration used by StreamForge.
    """

    # --------------------------------------------------------
    # Connection
    # --------------------------------------------------------

    bootstrap_servers: str = _get_env(
        "KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:9092",
    )

    client_id_prefix: str = _get_env(
        "KAFKA_CLIENT_ID_PREFIX",
        "streamforge",
    )

    # --------------------------------------------------------
    # Topics
    # --------------------------------------------------------

    telemetry_topic: str = _get_env(
        "KAFKA_TELEMETRY_TOPIC",
        "telemetry",
    )

    state_changelog_topic: str = _get_env(
        "KAFKA_STATE_CHANGELOG_TOPIC",
        "state-changelog",
    )

    temperature_aggregates_topic: str = _get_env(
        "KAFKA_TEMPERATURE_AGGREGATES_TOPIC",
        "temperature-aggregates",
    )

    worker_metrics_topic: str = _get_env(
        "KAFKA_WORKER_METRICS_TOPIC",
        "worker-metrics",
    )

    # --------------------------------------------------------
    # Consumer configuration
    # --------------------------------------------------------

    consumer_group: str = _get_env(
        "KAFKA_CONSUMER_GROUP",
        "streamforge-workers",
    )

    auto_offset_reset: str = _get_env(
        "KAFKA_AUTO_OFFSET_RESET",
        "earliest",
    )

    enable_auto_commit: bool = _get_bool(
        "KAFKA_ENABLE_AUTO_COMMIT",
        False,
    )

    # --------------------------------------------------------
    # Producer configuration
    # --------------------------------------------------------

    acks: str = _get_env(
        "KAFKA_ACKS",
        "all",
    )

    compression_type: str = _get_env(
        "KAFKA_COMPRESSION_TYPE",
        "gzip",
    )

    linger_ms: int = _get_int(
        "KAFKA_LINGER_MS",
        5,
    )

    batch_size: int = _get_int(
        "KAFKA_BATCH_SIZE",
        32768,
    )

    retries: int = _get_int(
        "KAFKA_RETRIES",
        10,
    )

    request_timeout_ms: int = _get_int(
        "KAFKA_REQUEST_TIMEOUT_MS",
        30000,
    )

    delivery_timeout_ms: int = _get_int(
        "KAFKA_DELIVERY_TIMEOUT_MS",
        120000,
    )

    # --------------------------------------------------------
    # Serialization
    # --------------------------------------------------------

    json_encoding: str = _get_env(
        "KAFKA_JSON_ENCODING",
        "utf-8",
    )


# ============================================================
# WORKER CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class WorkerSettings:
    """
    Configuration for StreamForge processing workers.
    """

    worker_count: int = _get_int(
        "STREAMFORGE_WORKER_COUNT",
        2,
    )

    worker_id_prefix: str = _get_env(
        "STREAMFORGE_WORKER_ID_PREFIX",
        "worker",
    )

    poll_timeout_ms: int = _get_int(
        "STREAMFORGE_POLL_TIMEOUT_MS",
        1000,
    )

    max_poll_records: int = _get_int(
        "STREAMFORGE_MAX_POLL_RECORDS",
        500,
    )

    max_poll_interval_ms: int = _get_int(
        "STREAMFORGE_MAX_POLL_INTERVAL_MS",
        300000,
    )

    session_timeout_ms: int = _get_int(
        "STREAMFORGE_SESSION_TIMEOUT_MS",
        45000,
    )

    heartbeat_interval_ms: int = _get_int(
        "STREAMFORGE_HEARTBEAT_INTERVAL_MS",
        15000,
    )

    shutdown_timeout_seconds: float = _get_float(
        "STREAMFORGE_SHUTDOWN_TIMEOUT_SECONDS",
        10.0,
    )

    processing_retry_count: int = _get_int(
        "STREAMFORGE_PROCESSING_RETRY_COUNT",
        3,
    )

    processing_retry_delay_seconds: float = _get_float(
        "STREAMFORGE_PROCESSING_RETRY_DELAY_SECONDS",
        1.0,
    )


# ============================================================
# AGGREGATION CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class AggregationSettings:
    """
    Configuration for telemetry aggregation.

    StreamForge uses a 5-minute temperature window with a
    1-minute slide by default.
    """

    # --------------------------------------------------------
    # Windowing
    # --------------------------------------------------------

    # Project specification:
    # 5-minute event-time aggregation window.
    window_seconds: int = _get_int(
        "STREAMFORGE_AGGREGATION_WINDOW_SECONDS",
        300,
    )

    # 1-minute slide creates rolling/hopping behavior.
    window_slide_seconds: int = _get_int(
        "STREAMFORGE_AGGREGATION_SLIDE_SECONDS",
        60,
    )

    # Events arriving within this period after the event-time
    # watermark are still accepted.
    allowed_lateness_seconds: int = _get_int(
        "STREAMFORGE_ALLOWED_LATENESS_SECONDS",
        10,
    )

    minimum_samples: int = _get_int(
        "STREAMFORGE_MINIMUM_SAMPLES",
        1,
    )

    # --------------------------------------------------------
    # Event fields
    # --------------------------------------------------------

    temperature_field: str = _get_env(
        "STREAMFORGE_TEMPERATURE_FIELD",
        "temperature",
    )

    device_id_field: str = _get_env(
        "STREAMFORGE_DEVICE_ID_FIELD",
        "device_id",
    )

    timestamp_field: str = _get_env(
        "STREAMFORGE_TIMESTAMP_FIELD",
        "timestamp",
    )


# ============================================================
# STATE CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class StateSettings:
    """
    Configuration for local worker state and recovery.
    """

    state_directory: Path = STATE_DIR

    state_backend: str = _get_env(
        "STREAMFORGE_STATE_BACKEND",
        "rocksdb",
    )

    checkpoint_interval_seconds: int = _get_int(
        "STREAMFORGE_CHECKPOINT_INTERVAL_SECONDS",
        10,
    )

    changelog_enabled: bool = _get_bool(
        "STREAMFORGE_CHANGELOG_ENABLED",
        True,
    )

    recovery_enabled: bool = _get_bool(
        "STREAMFORGE_RECOVERY_ENABLED",
        True,
    )

    recovery_poll_interval_seconds: float = _get_float(
        "STREAMFORGE_RECOVERY_POLL_INTERVAL_SECONDS",
        1.0,
    )

    clear_state_on_start: bool = _get_bool(
        "STREAMFORGE_CLEAR_STATE_ON_START",
        False,
    )


# ============================================================
# METRICS CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class MetricsSettings:
    """
    Prometheus metrics configuration.
    """

    enabled: bool = _get_bool(
        "STREAMFORGE_METRICS_ENABLED",
        True,
    )

    host: str = _get_env(
        "STREAMFORGE_METRICS_HOST",
        "127.0.0.1",
    )

    port: int = _get_int(
        "STREAMFORGE_METRICS_PORT",
        8001,
    )

    collection_interval_seconds: float = _get_float(
        "STREAMFORGE_METRICS_COLLECTION_INTERVAL_SECONDS",
        5.0,
    )


# ============================================================
# API CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class APISettings:
    """
    FastAPI configuration.
    """

    host: str = _get_env(
        "STREAMFORGE_API_HOST",
        "127.0.0.1",
    )

    port: int = _get_int(
        "STREAMFORGE_API_PORT",
        8000,
    )

    reload: bool = _get_bool(
        "STREAMFORGE_API_RELOAD",
        True,
    )

    title: str = _get_env(
        "STREAMFORGE_API_TITLE",
        "StreamForge API",
    )

    version: str = _get_env(
        "STREAMFORGE_API_VERSION",
        "1.0.0",
    )

    description: str = _get_env(
        "STREAMFORGE_API_DESCRIPTION",
        (
            "Real-time distributed stream processing "
            "platform built with Kafka and Python."
        ),
    )

    cors_enabled: bool = _get_bool(
        "STREAMFORGE_CORS_ENABLED",
        True,
    )


# ============================================================
# PRODUCER CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class ProducerSettings:
    """
    Telemetry producer configuration.
    """

    device_count: int = _get_int(
        "STREAMFORGE_DEVICE_COUNT",
        10,
    )

    events_per_second: float = _get_float(
        "STREAMFORGE_EVENTS_PER_SECOND",
        10.0,
    )

    interval_seconds: float = _get_float(
        "STREAMFORGE_PRODUCER_INTERVAL_SECONDS",
        0.1,
    )

    batch_size: int = _get_int(
        "STREAMFORGE_PRODUCER_BATCH_SIZE",
        100,
    )

    random_seed: int = _get_int(
        "STREAMFORGE_RANDOM_SEED",
        42,
    )

    temperature_min: float = _get_float(
        "STREAMFORGE_TEMPERATURE_MIN",
        15.0,
    )

    temperature_max: float = _get_float(
        "STREAMFORGE_TEMPERATURE_MAX",
        35.0,
    )


# ============================================================
# APPLICATION CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class ApplicationSettings:
    """
    Global StreamForge application settings.
    """

    name: str = _get_env(
        "STREAMFORGE_APP_NAME",
        "StreamForge",
    )

    environment: str = _get_env(
        "STREAMFORGE_ENVIRONMENT",
        "development",
    )

    log_level: str = _get_env(
        "STREAMFORGE_LOG_LEVEL",
        "INFO",
    )

    timezone: str = _get_env(
        "STREAMFORGE_TIMEZONE",
        "UTC",
    )

    version: str = _get_env(
        "STREAMFORGE_VERSION",
        "1.0.0",
    )


# ============================================================
# COMPLETE SETTINGS OBJECT
# ============================================================


@dataclass(frozen=True)
class Settings:
    """
    Complete StreamForge configuration.

    All application components should use this object
    rather than directly reading environment variables.
    """

    app: ApplicationSettings = ApplicationSettings()

    kafka: KafkaSettings = KafkaSettings()

    worker: WorkerSettings = WorkerSettings()

    aggregation: AggregationSettings = AggregationSettings()

    state: StateSettings = StateSettings()

    metrics: MetricsSettings = MetricsSettings()

    api: APISettings = APISettings()

    producer: ProducerSettings = ProducerSettings()


# ============================================================
# GLOBAL SETTINGS INSTANCE
# ============================================================


settings = Settings()


# ============================================================
# CONVENIENCE CONSTANTS
# ============================================================

# Kafka

KAFKA_BOOTSTRAP_SERVERS = (
    settings.kafka.bootstrap_servers
)

TELEMETRY_TOPIC = (
    settings.kafka.telemetry_topic
)

STATE_CHANGELOG_TOPIC = (
    settings.kafka.state_changelog_topic
)

TEMPERATURE_AGGREGATES_TOPIC = (
    settings.kafka.temperature_aggregates_topic
)

WORKER_METRICS_TOPIC = (
    settings.kafka.worker_metrics_topic
)

CONSUMER_GROUP = (
    settings.kafka.consumer_group
)


# Application

APP_NAME = settings.app.name

APP_VERSION = settings.app.version

ENVIRONMENT = settings.app.environment

LOG_LEVEL = settings.app.log_level


# API

API_HOST = settings.api.host

API_PORT = settings.api.port


# Metrics

METRICS_HOST = settings.metrics.host

METRICS_PORT = settings.metrics.port


# Paths

PROJECT_ROOT = PROJECT_DIR

DATA_PATH = DATA_DIR

STATE_PATH = STATE_DIR

LOG_PATH = LOG_DIR


# ============================================================
# VALIDATION
# ============================================================


def validate_settings() -> None:
    """
    Validate configuration values.

    Raises:
        ValueError: if configuration is invalid.
    """

    # --------------------------------------------------------
    # Kafka
    # --------------------------------------------------------

    if not settings.kafka.bootstrap_servers:
        raise ValueError(
            "Kafka bootstrap server cannot be empty."
        )

    if not settings.kafka.telemetry_topic:
        raise ValueError(
            "Telemetry topic cannot be empty."
        )

    if not settings.kafka.state_changelog_topic:
        raise ValueError(
            "State changelog topic cannot be empty."
        )

    if not settings.kafka.temperature_aggregates_topic:
        raise ValueError(
            "Temperature aggregates topic cannot be empty."
        )

    if not settings.kafka.worker_metrics_topic:
        raise ValueError(
            "Worker metrics topic cannot be empty."
        )

    if settings.kafka.acks not in {
        "0",
        "1",
        "all",
    }:
        raise ValueError(
            "Kafka acks must be one of: 0, 1, all."
        )

    if settings.kafka.linger_ms < 0:
        raise ValueError(
            "Kafka linger_ms cannot be negative."
        )

    if settings.kafka.batch_size <= 0:
        raise ValueError(
            "Kafka batch_size must be greater than 0."
        )

    if settings.kafka.retries < 0:
        raise ValueError(
            "Kafka retries cannot be negative."
        )

    # --------------------------------------------------------
    # Worker
    # --------------------------------------------------------

    if settings.worker.worker_count < 1:
        raise ValueError(
            "worker_count must be at least 1."
        )

    if settings.worker.poll_timeout_ms <= 0:
        raise ValueError(
            "poll_timeout_ms must be greater than 0."
        )

    if settings.worker.max_poll_records < 1:
        raise ValueError(
            "max_poll_records must be at least 1."
        )

    if settings.worker.max_poll_interval_ms <= 0:
        raise ValueError(
            "max_poll_interval_ms must be greater than 0."
        )

    if settings.worker.session_timeout_ms <= 0:
        raise ValueError(
            "session_timeout_ms must be greater than 0."
        )

    if settings.worker.heartbeat_interval_ms <= 0:
        raise ValueError(
            "heartbeat_interval_ms must be greater than 0."
        )

    if (
        settings.worker.heartbeat_interval_ms
        >= settings.worker.session_timeout_ms
    ):
        raise ValueError(
            "heartbeat_interval_ms must be less than "
            "session_timeout_ms."
        )

    if settings.worker.processing_retry_count < 0:
        raise ValueError(
            "processing_retry_count cannot be negative."
        )

    if settings.worker.processing_retry_delay_seconds < 0:
        raise ValueError(
            "processing_retry_delay_seconds cannot "
            "be negative."
        )

    # --------------------------------------------------------
    # Aggregation
    # --------------------------------------------------------

    if settings.aggregation.window_seconds <= 0:
        raise ValueError(
            "Aggregation window must be greater than 0."
        )

    if settings.aggregation.window_slide_seconds <= 0:
        raise ValueError(
            "Aggregation slide must be greater than 0."
        )

    if (
        settings.aggregation.window_slide_seconds
        > settings.aggregation.window_seconds
    ):
        raise ValueError(
            "Aggregation slide cannot be greater than "
            "the aggregation window."
        )

    if (
        settings.aggregation.window_seconds
        % settings.aggregation.window_slide_seconds
        != 0
    ):
        raise ValueError(
            "Aggregation window must be evenly divisible "
            "by the aggregation slide."
        )

    if (
        settings.aggregation.allowed_lateness_seconds
        < 0
    ):
        raise ValueError(
            "Allowed lateness cannot be negative."
        )

    if settings.aggregation.minimum_samples < 1:
        raise ValueError(
            "minimum_samples must be at least 1."
        )

    # --------------------------------------------------------
    # State
    # --------------------------------------------------------

    if settings.state.checkpoint_interval_seconds <= 0:
        raise ValueError(
            "Checkpoint interval must be greater than 0."
        )

    if settings.state.recovery_poll_interval_seconds <= 0:
        raise ValueError(
            "Recovery poll interval must be greater than 0."
        )

    if settings.state.state_backend.lower() != "rocksdb":
        raise ValueError(
            "StreamForge currently supports RocksDB "
            "as the state backend."
        )

    # --------------------------------------------------------
    # Metrics
    # --------------------------------------------------------

    if not (
        1 <= settings.metrics.port <= 65535
    ):
        raise ValueError(
            "Metrics port must be between 1 and 65535."
        )

    if settings.metrics.collection_interval_seconds <= 0:
        raise ValueError(
            "Metrics collection interval must be "
            "greater than 0."
        )

    # --------------------------------------------------------
    # API
    # --------------------------------------------------------

    if not (
        1 <= settings.api.port <= 65535
    ):
        raise ValueError(
            "API port must be between 1 and 65535."
        )

    # --------------------------------------------------------
    # Producer
    # --------------------------------------------------------

    if settings.producer.device_count < 1:
        raise ValueError(
            "device_count must be at least 1."
        )

    if settings.producer.events_per_second <= 0:
        raise ValueError(
            "events_per_second must be greater than 0."
        )

    if settings.producer.interval_seconds <= 0:
        raise ValueError(
            "producer interval must be greater than 0."
        )

    if settings.producer.batch_size < 1:
        raise ValueError(
            "producer batch_size must be at least 1."
        )

    if (
        settings.producer.temperature_min
        >= settings.producer.temperature_max
    ):
        raise ValueError(
            "temperature_min must be less than "
            "temperature_max."
        )


# ============================================================
# CONFIGURATION SUMMARY
# ============================================================


def get_config_summary() -> dict[str, object]:
    """
    Return a safe configuration summary.

    This intentionally excludes secrets because StreamForge
    currently has no authentication credentials in this
    configuration.
    """

    return {
        "application": {
            "name": settings.app.name,
            "version": settings.app.version,
            "environment": settings.app.environment,
            "log_level": settings.app.log_level,
            "timezone": settings.app.timezone,
        },

        "kafka": {
            "bootstrap_servers": (
                settings.kafka.bootstrap_servers
            ),
            "telemetry_topic": (
                settings.kafka.telemetry_topic
            ),
            "state_changelog_topic": (
                settings.kafka.state_changelog_topic
            ),
            "temperature_aggregates_topic": (
                settings.kafka.temperature_aggregates_topic
            ),
            "worker_metrics_topic": (
                settings.kafka.worker_metrics_topic
            ),
            "consumer_group": (
                settings.kafka.consumer_group
            ),
            "auto_offset_reset": (
                settings.kafka.auto_offset_reset
            ),
            "enable_auto_commit": (
                settings.kafka.enable_auto_commit
            ),
        },

        "workers": {
            "worker_count": (
                settings.worker.worker_count
            ),
            "max_poll_records": (
                settings.worker.max_poll_records
            ),
            "max_poll_interval_ms": (
                settings.worker.max_poll_interval_ms
            ),
            "session_timeout_ms": (
                settings.worker.session_timeout_ms
            ),
            "heartbeat_interval_ms": (
                settings.worker.heartbeat_interval_ms
            ),
        },

        "aggregation": {
            "window_seconds": (
                settings.aggregation.window_seconds
            ),
            "window_minutes": (
                settings.aggregation.window_seconds
                / 60
            ),
            "slide_seconds": (
                settings.aggregation.window_slide_seconds
            ),
            "allowed_lateness_seconds": (
                settings.aggregation.allowed_lateness_seconds
            ),
            "minimum_samples": (
                settings.aggregation.minimum_samples
            ),
        },

        "state": {
            "backend": (
                settings.state.state_backend
            ),
            "state_directory": str(
                settings.state.state_directory
            ),
            "checkpoint_interval_seconds": (
                settings.state.checkpoint_interval_seconds
            ),
            "changelog_enabled": (
                settings.state.changelog_enabled
            ),
            "recovery_enabled": (
                settings.state.recovery_enabled
            ),
        },

        "metrics": {
            "enabled": settings.metrics.enabled,
            "host": settings.metrics.host,
            "port": settings.metrics.port,
            "collection_interval_seconds": (
                settings.metrics.collection_interval_seconds
            ),
        },

        "api": {
            "host": settings.api.host,
            "port": settings.api.port,
            "reload": settings.api.reload,
            "cors_enabled": settings.api.cors_enabled,
        },

        "producer": {
            "device_count": (
                settings.producer.device_count
            ),
            "events_per_second": (
                settings.producer.events_per_second
            ),
            "batch_size": (
                settings.producer.batch_size
            ),
            "temperature_min": (
                settings.producer.temperature_min
            ),
            "temperature_max": (
                settings.producer.temperature_max
            ),
        },
    }


# ============================================================
# MODULE VALIDATION
# ============================================================

validate_settings()


# ============================================================
# COMMAND-LINE TEST
# ============================================================


if __name__ == "__main__":
    import json

    print(
        "StreamForge configuration loaded successfully."
    )

    print()

    print(
        json.dumps(
            get_config_summary(),
            indent=2,
            default=str,
        )
    )

    print()

    print(
        "Configuration validation: OK"
    )

    print(
        "Aggregation window: "
        f"{settings.aggregation.window_seconds} seconds "
        f"({settings.aggregation.window_seconds / 60:.0f} minutes)"
    )

    print(
        "Aggregation slide: "
        f"{settings.aggregation.window_slide_seconds} seconds"
    )