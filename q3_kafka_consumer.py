"""Consume and validate server metrics from Kafka."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

LOGGER = logging.getLogger(__name__)
STOP_REQUESTED = False


def configure_logging() -> None:
    """Configure concise structured-style application logs."""

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def parse_metric(raw_value: bytes | str) -> dict[str, Any] | None:
    """Parse and validate one Kafka payload, returning None when invalid."""

    try:
        payload = json.loads(raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        LOGGER.warning("Malformed Kafka message discarded")
        return None

    if not isinstance(payload, dict):
        LOGGER.warning("Kafka message must contain a JSON object")
        return None

    required_fields = ("server_id", "cpu_usage", "memory_usage", "disk_usage", "timestamp")
    if any(field not in payload for field in required_fields):
        LOGGER.warning("Kafka message missing required fields payload=%s", payload)
        return None
    if not isinstance(payload["server_id"], str) or not payload["server_id"].strip():
        LOGGER.warning("Kafka message has an invalid server_id payload=%s", payload)
        return None
    if not isinstance(payload["timestamp"], str) or not payload["timestamp"].strip():
        LOGGER.warning("Kafka message has an invalid timestamp payload=%s", payload)
        return None

    for field in ("cpu_usage", "memory_usage", "disk_usage"):
        value = payload[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 100:
            LOGGER.warning("Kafka message has an invalid %s payload=%s", field, payload)
            return None

    return payload


def process_metric(payload: dict[str, Any], cpu_threshold: float = 80.0) -> bool:
    """Log a metric and return whether it exceeds the CPU threshold."""

    server_id = payload["server_id"]
    cpu_usage = payload["cpu_usage"]
    LOGGER.info(
        "Metric received server_id=%s cpu_usage=%s memory_usage=%s disk_usage=%s timestamp=%s",
        server_id,
        cpu_usage,
        payload["memory_usage"],
        payload["disk_usage"],
        payload["timestamp"],
    )
    if cpu_usage > cpu_threshold:
        LOGGER.warning("ALERT: High CPU detected on %s cpu_usage=%s", server_id, cpu_usage)
        return True
    return False


def create_consumer() -> KafkaConsumer:
    """Create a consumer using environment-based Kafka configuration."""

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "server_metrics")
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "aiops-q3-consumer")
    if not topic or not group_id:
        raise ValueError("KAFKA_TOPIC and KAFKA_CONSUMER_GROUP cannot be empty")

    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )


def consume_metrics(max_messages: int | None = None) -> int:
    """Consume metrics until stopped, retrying temporary Kafka errors."""

    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages must be positive")

    threshold = float(os.getenv("CPU_ALERT_THRESHOLD", "80"))
    retry_delay_seconds = float(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "2"))
    processed_count = 0
    consumer = None
    try:
        consumer = create_consumer()
        while not STOP_REQUESTED and (max_messages is None or processed_count < max_messages):
            try:
                records = consumer.poll(timeout_ms=1000)
                for messages in records.values():
                    for message in messages:
                        payload = parse_metric(message.value)
                        if payload is None:
                            continue
                        process_metric(payload, cpu_threshold=threshold)
                        processed_count += 1
                        if max_messages is not None and processed_count >= max_messages:
                            break
                    if max_messages is not None and processed_count >= max_messages:
                        break
            except KafkaError:
                LOGGER.exception("Kafka consumer error; retrying")
                time.sleep(retry_delay_seconds)
    except KeyboardInterrupt:
        LOGGER.info("Consumer shutdown requested")
    finally:
        if consumer is not None:
            consumer.close()
        LOGGER.info("Consumer shut down metrics_processed=%d", processed_count)
    return processed_count


def stop_consumer(_signum: int, _frame: Any) -> None:
    """Request a clean consumer shutdown."""

    global STOP_REQUESTED
    STOP_REQUESTED = True


if __name__ == "__main__":
    configure_logging()
    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)
    consume_metrics()