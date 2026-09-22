"""
StreamForge API.

FastAPI monitoring and control API for the StreamForge
real-time stream-processing platform.

The API provides:

    GET /
    GET /health
    GET /ready
    GET /metrics
    GET /workers
    GET /workers/{worker_id}
    GET /topology
    GET /stats
    GET /config
    GET /state

The API is designed for the StreamForge frontend and
monitoring dashboard.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

from .config import (
    APP_NAME,
    APP_VERSION,
    KAFKA_BOOTSTRAP_SERVERS,
    TELEMETRY_TOPIC,
    STATE_CHANGELOG_TOPIC,
    TEMPERATURE_AGGREGATES_TOPIC,
    WORKER_METRICS_TOPIC,
)

from .metrics import metrics_manager


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# APPLICATION
# ============================================================

app = FastAPI(
    title="StreamForge API",
    description=(
        "Monitoring and control API for the "
        "StreamForge real-time stream-processing platform."
    ),
    version=APP_VERSION,
)


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# START TIME
# ============================================================

START_TIME = time.time()


# ============================================================
# RESPONSE MODELS
# ============================================================


class APIStatus(BaseModel):
    """Basic API status."""

    status: str
    service: str
    version: str
    uptime_seconds: float


class HealthResponse(BaseModel):
    """API health response."""

    status: str
    kafka: str
    workers: int
    active_workers: int
    metrics: str
    uptime_seconds: float


# ============================================================
# HELPER FUNCTIONS
# ============================================================


def get_uptime() -> float:
    """Return API uptime in seconds."""

    return max(
        0.0,
        time.time() - START_TIME,
    )


def get_kafka_health() -> Dict[str, Any]:
    """
    Check whether Kafka is reachable.

    A short-lived KafkaAdminClient is used so that the API
    does not maintain a permanent administrative connection.
    """

    try:

        from kafka import KafkaAdminClient

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="streamforge-api-health",
            request_timeout_ms=3000,
        )

        try:

            topics = admin_client.list_topics()

        finally:

            admin_client.close()

        return {
            "status": "healthy",
            "topics": sorted(
                list(topics)
            ),
        }

    except Exception as exc:

        logger.warning(
            "Kafka health check failed: %s",
            exc,
        )

        return {
            "status": "unhealthy",
            "topics": [],
            "error": str(exc),
        }


def get_required_topics() -> Dict[str, Dict[str, Any]]:
    """
    Return the StreamForge Kafka topology definition.
    """

    return {
        TELEMETRY_TOPIC: {
            "purpose": "Incoming device telemetry",
            "expected_partitions": 8,
            "cleanup_policy": "delete",
        },

        STATE_CHANGELOG_TOPIC: {
            "purpose": "Persistent worker state changelog",
            "expected_partitions": 8,
            "cleanup_policy": "compact",
        },

        TEMPERATURE_AGGREGATES_TOPIC: {
            "purpose": "Temperature window aggregates",
            "expected_partitions": 8,
            "cleanup_policy": "compact",
        },

        WORKER_METRICS_TOPIC: {
            "purpose": "Worker runtime metrics",
            "expected_partitions": 1,
            "cleanup_policy": "delete",
        },
    }


def get_topic_partition_counts() -> Dict[str, int]:
    """Return actual Kafka partition counts."""

    try:

        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="streamforge-api-topology",
            request_timeout_ms=3000,
        )

        try:

            result: Dict[str, int] = {}

            for topic_name in get_required_topics():

                partitions = (
                    consumer.partitions_for_topic(
                        topic_name
                    )
                )

                if partitions is None:
                    result[topic_name] = 0
                else:
                    result[topic_name] = len(
                        partitions
                    )

            return result

        finally:

            consumer.close()

    except Exception as exc:

        logger.warning(
            "Unable to query Kafka partition counts: %s",
            exc,
        )

        return {}


def get_registered_workers() -> Dict[str, Any]:
    """Return registered workers from MetricsManager."""

    try:

        workers = metrics_manager.get_workers()

        if isinstance(workers, dict):
            return workers

        return {}

    except Exception as exc:

        logger.exception(
            "Unable to obtain worker metrics."
        )

        return {
            "error": str(exc)
        }


def get_active_worker_count(
    workers: Dict[str, Any],
) -> int:
    """Count currently running workers."""

    count = 0

    for worker in workers.values():

        if not isinstance(
            worker,
            dict,
        ):
            continue

        if worker.get(
            "running",
            False,
        ):
            count += 1

    return count


# ============================================================
# ROOT
# ============================================================


@app.get(
    "/",
    response_model=APIStatus,
)
def root() -> APIStatus:
    """Return StreamForge API information."""

    return APIStatus(
        status="running",
        service=APP_NAME,
        version=APP_VERSION,
        uptime_seconds=get_uptime(),
    )


# ============================================================
# HEALTH
# ============================================================


@app.get(
    "/health",
    response_model=HealthResponse,
)
def health() -> HealthResponse:
    """Return StreamForge health status."""

    kafka = get_kafka_health()

    workers = get_registered_workers()

    try:

        metrics_health = (
            metrics_manager.health()
        )

    except Exception:

        metrics_health = {
            "enabled": False,
            "workers_registered": 0,
            "active_workers": 0,
        }

    kafka_healthy = (
        kafka.get("status")
        == "healthy"
    )

    metrics_enabled = bool(
        metrics_health.get(
            "enabled",
            False,
        )
    )

    worker_count = len(
        workers
    )

    active_workers = get_active_worker_count(
        workers
    )

    return HealthResponse(
        status=(
            "healthy"
            if kafka_healthy
            else "degraded"
        ),

        kafka=(
            "healthy"
            if kafka_healthy
            else "unhealthy"
        ),

        workers=worker_count,

        active_workers=active_workers,

        metrics=(
            "healthy"
            if metrics_enabled
            else "disabled"
        ),

        uptime_seconds=get_uptime(),
    )


# ============================================================
# READINESS
# ============================================================


@app.get(
    "/ready",
)
def ready() -> Dict[str, Any]:
    """
    Return whether StreamForge is ready to process data.
    """

    kafka = get_kafka_health()

    if kafka.get(
        "status"
    ) != "healthy":

        raise HTTPException(
            status_code=503,
            detail={
                "status": "not_ready",
                "reason": "Kafka is unavailable",
            },
        )

    available_topics = set(
        kafka.get(
            "topics",
            [],
        )
    )

    required_topics = set(
        get_required_topics()
    )

    missing_topics = sorted(
        required_topics
        - available_topics
    )

    if missing_topics:

        raise HTTPException(
            status_code=503,
            detail={
                "status": "not_ready",
                "reason": "Required Kafka topics are missing",
                "missing_topics": missing_topics,
            },
        )

    return {
        "status": "ready",
        "kafka": "healthy",
        "topics": "ready",
    }


# ============================================================
# PROMETHEUS METRICS
# ============================================================


@app.get(
    "/metrics",
)
def prometheus_metrics() -> Response:
    """
    Return Prometheus-compatible metrics.
    """

    output = (
        metrics_manager.prometheus_output()
    )

    return Response(
        content=output,
        media_type=(
            "text/plain; version=0.0.4; "
            "charset=utf-8"
        ),
    )


# ============================================================
# WORKERS
# ============================================================


@app.get(
    "/workers",
)
def workers() -> Dict[str, Any]:
    """Return all registered StreamForge workers."""

    worker_data = (
        get_registered_workers()
    )

    return {
        "workers": worker_data,
        "count": len(worker_data),
        "active": get_active_worker_count(
            worker_data
        ),
    }


# ============================================================
# SINGLE WORKER
# ============================================================


@app.get(
    "/workers/{worker_id}",
)
def worker(
    worker_id: str,
) -> Dict[str, Any]:
    """Return information about one worker."""

    try:

        worker_data = (
            metrics_manager.get_worker(
                worker_id
            )
        )

    except Exception as exc:

        logger.exception(
            "Failed to obtain worker '%s'.",
            worker_id,
        )

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc

    if worker_data is None:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Worker not found: {worker_id}"
            ),
        )

    return worker_data


# ============================================================
# TOPOLOGY
# ============================================================


@app.get(
    "/topology",
)
def topology() -> Dict[str, Any]:
    """
    Return StreamForge's complete processing topology.

    This endpoint is consumed by the frontend topology
    visualization.
    """

    definitions = (
        get_required_topics()
    )

    actual_partitions = (
        get_topic_partition_counts()
    )

    topics = []

    for (
        topic_name,
        definition,
    ) in definitions.items():

        topics.append(
            {
                "name": topic_name,

                "purpose":
                    definition["purpose"],

                "expected_partitions":
                    definition[
                        "expected_partitions"
                    ],

                "partitions":
                    actual_partitions.get(
                        topic_name,
                        definition[
                            "expected_partitions"
                        ],
                    ),

                "cleanup_policy":
                    definition[
                        "cleanup_policy"
                    ],
            }
        )

    return {
        "service": APP_NAME,

        "topology": {

            "producer": {
                "name": "Telemetry Producer",
                "output": TELEMETRY_TOPIC,
            },

            "workers": {
                "name": "Stream Processing Workers",
                "input": TELEMETRY_TOPIC,
                "output": TEMPERATURE_AGGREGATES_TOPIC,
            },

            "state": {
                "name": "Persistent State",
                "changelog": STATE_CHANGELOG_TOPIC,
            },

            "metrics": {
                "name": "Worker Metrics",
                "topic": WORKER_METRICS_TOPIC,
            },
        },

        "topics": topics,
    }


# ============================================================
# APPLICATION STATISTICS
# ============================================================


@app.get(
    "/stats",
)
def stats() -> Dict[str, Any]:
    """Return application-wide statistics."""

    try:

        summary = (
            metrics_manager.get_summary()
        )

    except Exception as exc:

        logger.exception(
            "Unable to obtain metrics summary."
        )

        summary = {
            "error": str(exc)
        }

    return {
        "service": APP_NAME,
        "version": APP_VERSION,
        "uptime_seconds": get_uptime(),
        "statistics": summary,
    }


# ============================================================
# SAFE CONFIGURATION
# ============================================================


@app.get(
    "/config",
)
def configuration() -> Dict[str, Any]:
    """
    Return non-sensitive StreamForge configuration.

    Secrets and credentials are intentionally not exposed.
    """

    return {
        "application": {
            "name": APP_NAME,
            "version": APP_VERSION,
        },

        "kafka": {
            "bootstrap_servers":
                KAFKA_BOOTSTRAP_SERVERS,

            "telemetry_topic":
                TELEMETRY_TOPIC,

            "state_changelog_topic":
                STATE_CHANGELOG_TOPIC,

            "temperature_aggregates_topic":
                TEMPERATURE_AGGREGATES_TOPIC,

            "worker_metrics_topic":
                WORKER_METRICS_TOPIC,
        },

        "workers": {
            "configured":
                _get_configured_worker_count(),
        },
    }


def _get_configured_worker_count() -> int:
    """
    Read worker_count from config without assuming a specific
    constant name.

    This keeps api.py compatible with the current config.py.
    """

    try:

        from . import config

        value = getattr(
            config,
            "WORKER_COUNT",
            None,
        )

        if value is not None:
            return int(value)

        value = getattr(
            config,
            "WORKERS",
            None,
        )

        if isinstance(
            value,
            dict,
        ):

            value = value.get(
                "worker_count",
                0,
            )

            return int(value)

        if value is not None:
            return int(value)

        return 0

    except Exception:

        return 0


# ============================================================
# APPLICATION STATE
# ============================================================


@app.get(
    "/state",
)
def application_state() -> Dict[str, Any]:
    """
    Return high-level StreamForge runtime state.

    Detailed RocksDB contents are intentionally not exposed.
    """

    kafka = get_kafka_health()

    workers = get_registered_workers()

    try:

        metrics_health = (
            metrics_manager.health()
        )

    except Exception as exc:

        metrics_health = {
            "enabled": False,
            "error": str(exc),
        }

    return {
        "api": {
            "running": True,
            "uptime_seconds": get_uptime(),
        },

        "kafka": kafka,

        "workers": {
            "registered":
                len(workers),

            "active":
                get_active_worker_count(
                    workers
                ),

            "configured":
                _get_configured_worker_count(),
        },

        "metrics":
            metrics_health,

        "status": (
            "healthy"
            if kafka.get("status")
            == "healthy"
            else "degraded"
        ),
    }


# ============================================================
# SELF TEST
# ============================================================


def run_self_test() -> None:
    """Run API self-tests without starting Uvicorn."""

    print(
        "StreamForge API self-test"
    )

    print(
        "-" * 60
    )

    # --------------------------------------------------------
    # Application
    # --------------------------------------------------------

    assert app.title == (
        "StreamForge API"
    )

    assert app.version == (
        APP_VERSION
    )

    print(
        "FastAPI application: OK"
    )

    # --------------------------------------------------------
    # Routes
    # --------------------------------------------------------

    routes = {
        route.path
        for route in app.routes
    }

    expected_routes = {
        "/",
        "/health",
        "/ready",
        "/metrics",
        "/workers",
        "/workers/{worker_id}",
        "/topology",
        "/stats",
        "/config",
        "/state",
    }

    for route in expected_routes:

        assert route in routes, (
            f"Missing route: {route}"
        )

    print(
        "API routes: OK"
    )

    # --------------------------------------------------------
    # Kafka topology
    # --------------------------------------------------------

    definitions = (
        get_required_topics()
    )

    assert TELEMETRY_TOPIC in definitions
    assert STATE_CHANGELOG_TOPIC in definitions
    assert TEMPERATURE_AGGREGATES_TOPIC in definitions
    assert WORKER_METRICS_TOPIC in definitions

    print(
        "Kafka topology definition: OK"
    )

    # --------------------------------------------------------
    # Configuration
    # --------------------------------------------------------

    configured_workers = (
        _get_configured_worker_count()
    )

    assert isinstance(
        configured_workers,
        int,
    )

    assert configured_workers >= 0

    print(
        "Configuration integration: OK"
    )

    # --------------------------------------------------------
    # Metrics integration
    # --------------------------------------------------------

    test_worker_id = (
        "api-self-test-worker"
    )

    metrics_manager.register_worker(
        test_worker_id
    )

    metrics_manager.record_message_received(
        test_worker_id,
        bytes_count=100,
    )

    metrics_manager.record_message_processed(
        test_worker_id,
        processing_seconds=0.01,
    )

    worker_data = (
        metrics_manager.get_worker(
            test_worker_id
        )
    )

    assert worker_data is not None

    summary = (
        metrics_manager.get_summary()
    )

    assert summary[
        "messages_received"
    ] >= 1

    assert summary[
        "messages_processed"
    ] >= 1

    metrics_manager.unregister_worker(
        test_worker_id
    )

    try:
        metrics_manager.clear()
    except Exception:
        pass

    print(
        "Metrics integration: OK"
    )

    # --------------------------------------------------------
    # Uptime
    # --------------------------------------------------------

    assert get_uptime() >= 0

    print(
        "Uptime calculation: OK"
    )

    # --------------------------------------------------------
    # Final
    # --------------------------------------------------------

    print(
        "-" * 60
    )

    print(
        "StreamForge API self-test: OK"
    )


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    """Start the FastAPI server."""

    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    logger.info(
        "Starting StreamForge API..."
    )

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )


if __name__ == "__main__":
    main()