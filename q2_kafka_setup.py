"""Create the Kafka topic used by the AIOps metric pipeline."""

from __future__ import annotations

import logging
import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure concise structured-style application logs."""

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def create_topic(
    broker: str | None = None,
    topic: str | None = None,
    retries: int = 10,
    retry_delay_seconds: float = 2.0,
) -> None:
    """Create a topic, tolerating an existing topic and startup delay."""

    broker = broker if broker is not None else os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = topic if topic is not None else os.getenv("KAFKA_TOPIC", "server_metrics")
    partitions = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
    replication_factor = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1"))

    if not topic:
        raise ValueError("KAFKA_TOPIC cannot be empty")
    if partitions < 1 or replication_factor < 1:
        raise ValueError("Kafka partitions and replication factor must be positive")

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        admin = None
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, client_id="aiops-topic-setup")
            admin.create_topics(
                [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)],
                validate_only=False,
            )
            LOGGER.info("Kafka topic created topic=%s broker=%s", topic, broker)
            return
        except TopicAlreadyExistsError:
            LOGGER.info("Kafka topic already exists topic=%s", topic)
            return
        except Exception as error:
            last_error = error
            LOGGER.warning(
                "Kafka topic setup attempt failed attempt=%d/%d broker=%s error=%s",
                attempt,
                retries,
                broker,
                error,
            )
            if attempt < retries:
                time.sleep(retry_delay_seconds)
        finally:
            if admin is not None:
                admin.close()

    raise RuntimeError(f"Unable to create Kafka topic {topic!r}") from last_error


if __name__ == "__main__":
    configure_logging()
    create_topic()