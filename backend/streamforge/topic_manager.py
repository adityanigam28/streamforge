"""
StreamForge Kafka Topic Manager

Responsible for creating and validating the Kafka topology
used by StreamForge.

Architecture:

    Telemetry Producers
            |
            v
       [telemetry]
            |
            v
       Stream Workers
         /       \
        v         v
[state-changelog] [temperature-aggregates]
        |
        v
   Recovery System

Worker metrics are published to:

    [worker-metrics]


Required topics:

    telemetry
        8 partitions
        Normal Kafka topic

    state-changelog
        8 partitions
        cleanup.policy=compact

    temperature-aggregates
        8 partitions
        cleanup.policy=compact

    worker-metrics
        1 partition
        Normal Kafka topic


Run from the backend directory:

    python -m streamforge.topic_manager
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import (
    KafkaError,
    NoBrokersAvailable,
    NodeNotReadyError,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv(
        "STREAMFORGE_LOG_LEVEL",
        "INFO",
    ).upper(),
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger(
    "streamforge.topic_manager"
)


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9092",
)

KAFKA_CLIENT_ID = os.getenv(
    "KAFKA_ADMIN_CLIENT_ID",
    "streamforge-topic-manager",
)

CONNECTION_TIMEOUT_MS = int(
    os.getenv(
        "KAFKA_CONNECTION_TIMEOUT_MS",
        "10000",
    )
)

REQUEST_TIMEOUT_MS = int(
    os.getenv(
        "KAFKA_REQUEST_TIMEOUT_MS",
        "10000",
    )
)

RETRY_INTERVAL_SECONDS = float(
    os.getenv(
        "KAFKA_RETRY_INTERVAL_SECONDS",
        "2",
    )
)

MAX_RETRIES = int(
    os.getenv(
        "KAFKA_MAX_RETRIES",
        "30",
    )
)

# Docker Kafka container used by StreamForge.
KAFKA_CONTAINER = os.getenv(
    "KAFKA_CONTAINER",
    "streamforge-kafka",
)

# Kafka's internal listener.
KAFKA_INTERNAL_BOOTSTRAP = os.getenv(
    "KAFKA_INTERNAL_BOOTSTRAP",
    "kafka:29092",
)


# ============================================================
# TOPIC DEFINITION
# ============================================================

@dataclass(frozen=True)
class TopicDefinition:
    """
    Definition of a StreamForge Kafka topic.
    """

    name: str
    partitions: int
    replication_factor: int = 1
    cleanup_policy: str | None = None


# ============================================================
# STREAMFORGE TOPOLOGY
# ============================================================

TOPICS: tuple[TopicDefinition, ...] = (
    TopicDefinition(
        name="telemetry",
        partitions=8,
        replication_factor=1,
        cleanup_policy=None,
    ),

    TopicDefinition(
        name="state-changelog",
        partitions=8,
        replication_factor=1,
        cleanup_policy="compact",
    ),

    TopicDefinition(
        name="temperature-aggregates",
        partitions=8,
        replication_factor=1,
        cleanup_policy="compact",
    ),

    TopicDefinition(
        name="worker-metrics",
        partitions=1,
        replication_factor=1,
        cleanup_policy=None,
    ),
)


# ============================================================
# ADMIN CLIENT
# ============================================================

def create_admin_client() -> KafkaAdminClient:
    """
    Create a Kafka admin client.
    """

    return KafkaAdminClient(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        client_id=KAFKA_CLIENT_ID,
        request_timeout_ms=REQUEST_TIMEOUT_MS,
        api_version_auto_timeout_ms=CONNECTION_TIMEOUT_MS,
        connections_max_idle_ms=30000,
    )


# ============================================================
# WAIT FOR KAFKA
# ============================================================

def wait_for_kafka() -> KafkaAdminClient:
    """
    Wait until Kafka is reachable.
    """

    logger.info(
        "Waiting for Kafka at %s...",
        BOOTSTRAP_SERVERS,
    )

    last_error: Exception | None = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        admin_client = None

        try:

            admin_client = create_admin_client()

            admin_client.list_topics()

            logger.info(
                "Kafka connection established."
            )

            return admin_client

        except (
            NoBrokersAvailable,
            NodeNotReadyError,
            KafkaError,
        ) as exc:

            last_error = exc

            logger.warning(
                "Kafka not ready "
                "(attempt %d/%d): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )

            if admin_client is not None:

                try:
                    admin_client.close()
                except Exception:
                    pass

            time.sleep(
                RETRY_INTERVAL_SECONDS
            )

        except Exception as exc:

            last_error = exc

            logger.warning(
                "Unexpected Kafka error "
                "(attempt %d/%d): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )

            if admin_client is not None:

                try:
                    admin_client.close()
                except Exception:
                    pass

            time.sleep(
                RETRY_INTERVAL_SECONDS
            )

    raise RuntimeError(
        "Kafka did not become ready after "
        f"{MAX_RETRIES} attempts. "
        f"Last error: {last_error}"
    )


# ============================================================
# BUILD TOPIC
# ============================================================

def build_new_topic(
    definition: TopicDefinition,
) -> NewTopic:
    """
    Convert a TopicDefinition to a Kafka NewTopic object.
    """

    topic = NewTopic(
        name=definition.name,
        num_partitions=definition.partitions,
        replication_factor=definition.replication_factor,
    )

    if definition.cleanup_policy is not None:

        topic.topic_configs = {
            "cleanup.policy": definition.cleanup_policy,
        }

    return topic


# ============================================================
# GET EXISTING TOPICS
# ============================================================

def get_existing_topics(
    admin_client: KafkaAdminClient,
) -> set[str]:
    """
    Return all existing Kafka topic names.
    """

    last_error: Exception | None = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            return set(
                admin_client.list_topics()
            )

        except (
            NodeNotReadyError,
            KafkaError,
        ) as exc:

            last_error = exc

            logger.warning(
                "Kafka metadata unavailable "
                "(attempt %d/%d): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )

            time.sleep(
                RETRY_INTERVAL_SECONDS
            )

    raise RuntimeError(
        "Unable to retrieve Kafka topics. "
        f"Last error: {last_error}"
    )


# ============================================================
# CREATE MISSING TOPICS
# ============================================================

def create_topics(
    admin_client: KafkaAdminClient,
) -> None:
    """
    Create all missing StreamForge topics.

    Existing topics are never deleted.
    """

    existing_topics = get_existing_topics(
        admin_client
    )

    missing_topics: list[NewTopic] = []

    for definition in TOPICS:

        if definition.name in existing_topics:

            logger.info(
                "Topic already exists: %s",
                definition.name,
            )

            continue

        logger.info(
            "Creating topic: %s "
            "(partitions=%d, replication_factor=%d)",
            definition.name,
            definition.partitions,
            definition.replication_factor,
        )

        missing_topics.append(
            build_new_topic(definition)
        )

    if not missing_topics:

        logger.info(
            "All required StreamForge topics "
            "already exist."
        )

        return

    try:

        response = admin_client.create_topics(
            new_topics=missing_topics,
            validate_only=False,
        )

        logger.debug(
            "Kafka create-topics response: %r",
            response,
        )

        logger.info(
            "Kafka topic creation completed."
        )

    except KafkaError as exc:

        logger.error(
            "Failed to create Kafka topics: %s",
            exc,
        )

        raise


# ============================================================
# ENSURE COMPACTION
# ============================================================

def ensure_compaction(
    topic_name: str,
) -> bool:
    """
    Ensure a topic has cleanup.policy=compact.

    Uses Kafka's official kafka-configs.sh command inside
    the running Kafka container.

    This avoids kafka-python version-specific differences
    in KafkaAdminClient.describe_configs().
    """

    command = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        KAFKA_INTERNAL_BOOTSTRAP,
        "--entity-type",
        "topics",
        "--entity-name",
        topic_name,
        "--alter",
        "--add-config",
        "cleanup.policy=compact",
    ]

    logger.info(
        "Ensuring compact cleanup policy for: %s",
        topic_name,
    )

    try:

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    except FileNotFoundError:

        logger.error(
            "Docker command was not found. "
            "Make sure Docker Desktop is running."
        )

        return False

    except subprocess.TimeoutExpired:

        logger.error(
            "Timed out while configuring topic '%s'.",
            topic_name,
        )

        return False

    if result.returncode != 0:

        logger.error(
            "Failed to configure topic '%s'.",
            topic_name,
        )

        if result.stderr.strip():

            logger.error(
                "Kafka config error: %s",
                result.stderr.strip(),
            )

        return False

    logger.info(
        "Compaction enabled for topic: %s",
        topic_name,
    )

    return True


# ============================================================
# GET PARTITION COUNT
# ============================================================

def get_partition_count(
    admin_client: KafkaAdminClient,
    topic_name: str,
) -> int:
    """
    Return the partition count for a topic.
    """

    metadata = admin_client.describe_topics(
        topics=[topic_name]
    )

    if not metadata:

        raise RuntimeError(
            f"No metadata returned for "
            f"topic '{topic_name}'."
        )

    partitions = metadata[0].get(
        "partitions",
        [],
    )

    return len(partitions)


# ============================================================
# VALIDATE PARTITIONS
# ============================================================

def validate_partitions(
    admin_client: KafkaAdminClient,
    definition: TopicDefinition,
) -> bool:
    """
    Validate partition count for one topic.
    """

    actual = get_partition_count(
        admin_client,
        definition.name,
    )

    if actual != definition.partitions:

        logger.error(
            "Topic '%s' has %d partitions; "
            "expected %d.",
            definition.name,
            actual,
            definition.partitions,
        )

        return False

    return True


# ============================================================
# VALIDATE TOPOLOGY
# ============================================================

def validate_topology(
    admin_client: KafkaAdminClient,
) -> bool:
    """
    Validate the complete StreamForge Kafka topology.
    """

    logger.info(
        "Validating StreamForge Kafka topology..."
    )

    all_valid = True

    existing_topics = get_existing_topics(
        admin_client
    )

    # --------------------------------------------------------
    # Check every topic
    # --------------------------------------------------------

    for definition in TOPICS:

        logger.info(
            "Validating topic: %s",
            definition.name,
        )

        if definition.name not in existing_topics:

            logger.error(
                "Missing topic: %s",
                definition.name,
            )

            all_valid = False

            continue

        # ----------------------------------------------------
        # Partition validation
        # ----------------------------------------------------

        try:

            if not validate_partitions(
                admin_client,
                definition,
            ):

                all_valid = False

                continue

        except Exception as exc:

            logger.error(
                "Could not validate partitions "
                "for '%s': %s",
                definition.name,
                exc,
            )

            all_valid = False

            continue

        # ----------------------------------------------------
        # Compaction
        # ----------------------------------------------------

        if definition.cleanup_policy == "compact":

            if not ensure_compaction(
                definition.name
            ):

                all_valid = False

                continue

        logger.info(
            "Topic validated successfully: %s",
            definition.name,
        )

    return all_valid


# ============================================================
# PRINT TOPOLOGY
# ============================================================

def print_topology(
    admin_client: KafkaAdminClient,
) -> None:
    """
    Print the StreamForge Kafka topology.
    """

    logger.info("")
    logger.info("=" * 78)
    logger.info("StreamForge Kafka Topology")
    logger.info("=" * 78)

    for definition in TOPICS:

        try:

            partitions = get_partition_count(
                admin_client,
                definition.name,
            )

            if definition.cleanup_policy == "compact":

                cleanup = "compact"

            else:

                cleanup = "default"

            logger.info(
                "%-32s | partitions=%-2d | cleanup=%s",
                definition.name,
                partitions,
                cleanup,
            )

        except Exception as exc:

            logger.error(
                "%-32s | ERROR: %s",
                definition.name,
                exc,
            )

    logger.info("=" * 78)
    logger.info("")


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    """
    Main StreamForge Topic Manager.
    """

    admin_client: KafkaAdminClient | None = None

    try:

        logger.info(
            "Starting StreamForge Topic Manager."
        )

        logger.info(
            "Kafka bootstrap server: %s",
            BOOTSTRAP_SERVERS,
        )

        # ----------------------------------------------------
        # Connect to Kafka
        # ----------------------------------------------------

        admin_client = wait_for_kafka()

        # ----------------------------------------------------
        # Create missing topics
        # ----------------------------------------------------

        create_topics(
            admin_client
        )

        # ----------------------------------------------------
        # Allow metadata to settle
        # ----------------------------------------------------

        time.sleep(2)

        # ----------------------------------------------------
        # Validate topology
        # ----------------------------------------------------

        valid = validate_topology(
            admin_client
        )

        # ----------------------------------------------------
        # Print topology
        # ----------------------------------------------------

        print_topology(
            admin_client
        )

        if not valid:

            logger.error(
                "StreamForge Kafka topology "
                "validation failed."
            )

            return 1

        logger.info(
            "StreamForge Kafka topology is ready."
        )

        return 0

    except KeyboardInterrupt:

        logger.warning(
            "Topic Manager interrupted."
        )

        return 130

    except Exception as exc:

        logger.exception(
            "Topic Manager failed: %s",
            exc,
        )

        return 1

    finally:

        if admin_client is not None:

            try:
                admin_client.close()
            except Exception:
                pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())