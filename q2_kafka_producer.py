"""Publish sample server metrics to Kafka as JSON messages."""

from __future__ import annotations

import json
import logging
import os
import random
import signal
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from q2_kafka_setup import create_topic

LOGGER = logging.getLogger(__name__)
STOP_REQUESTED = False


def configure_logging() -> None:
    """Configure concise structured-style application logs."""

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def generate_metric(server_id: str, random_generator: random.Random | None = None) -> dict[str, Any]:
    """Generate one valid server metric payload."""

    random_generator = random_generator or random
    return {
        "server_id": server_id,
        "cpu_usage": random_generator.randint(20, 95),
        "memory_usage": random_generator.randint(30, 90),
        "disk_usage": random_generator.randint(20, 85),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def stop_producer(_signum: int, _frame: Any) -> None:
    """Request a clean producer shutdown after the current send."""

    global STOP_REQUESTED
    STOP_REQUESTED = True


def publish_metrics(message_count: int | None = None) -> int:
    """Publish metrics and wait for Kafka acknowledgement for every message."""

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "server_metrics")
    server_id = os.getenv("SERVER_ID", "server01")
    message_count = (
        message_count if message_count is not None else int(os.getenv("KAFKA_MESSAGE_COUNT", "10"))
    )
    interval_seconds = float(os.getenv("KAFKA_PRODUCER_INTERVAL_SECONDS", "1"))

    if message_count < 1:
        raise ValueError("KAFKA_MESSAGE_COUNT must be positive")

    create_topic(broker=broker, topic=topic)
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    sent_count = 0
    try:
        for _ in range(message_count):
            if STOP_REQUESTED:
                break
            metric = generate_metric(server_id)
            try:
                producer.send(topic, value=metric).get(timeout=10)
            except KafkaError:
                LOGGER.exception("Kafka publish failed topic=%s metric=%s", topic, metric)
                raise
            sent_count += 1
            LOGGER.info("Metric published topic=%s metric=%s", topic, metric)
            if sent_count < message_count and interval_seconds > 0:
                import time

                time.sleep(interval_seconds)
    finally:
        producer.flush(timeout=10)
        producer.close()
        LOGGER.info("Producer shut down messages_published=%d", sent_count)
    return sent_count


if __name__ == "__main__":
    configure_logging()
    signal.signal(signal.SIGINT, stop_producer)
    signal.signal(signal.SIGTERM, stop_producer)
    publish_metrics()