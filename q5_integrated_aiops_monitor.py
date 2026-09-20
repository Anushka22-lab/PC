"""Integrated Kafka-based AIOps monitoring service."""

from __future__ import annotations

import logging
import os
import signal
import time
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from q3_kafka_consumer import parse_metric, process_metric

LOGGER = logging.getLogger(__name__)
STOP_REQUESTED = False


def configure_logging() -> None:
    """Configure concise structured-style application logs."""

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


class AIOpsMonitor:
    """Consume, analyze, alert on, and report server metrics."""

    def __init__(self, consumer: KafkaConsumer | None = None) -> None:
        self.consumer = consumer
        self.cpu_threshold = float(os.getenv("CPU_ALERT_THRESHOLD", "80"))
        self.retry_delay_seconds = float(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "2"))
        self.anomaly_count = 0
        self.processed_count = 0
        if not 0 <= self.cpu_threshold <= 100:
            raise ValueError("CPU_ALERT_THRESHOLD must be between 0 and 100")

    def create_consumer(self) -> KafkaConsumer:
        """Create the integrated monitor's dedicated consumer group."""

        broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        topic = os.getenv("KAFKA_TOPIC", "server_metrics")
        group_id = os.getenv("KAFKA_CONSUMER_GROUP", "aiops-q5-monitor")
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

    def connect_with_retries(self) -> KafkaConsumer:
        """Connect to Kafka while tolerating temporary broker startup failures."""

        max_attempts = int(os.getenv("KAFKA_CONNECTION_RETRIES", "10"))
        last_error: KafkaError | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return self.create_consumer()
            except KafkaError as error:
                last_error = error
                LOGGER.warning(
                    "Kafka connection failed attempt=%d/%d error=%s",
                    attempt,
                    max_attempts,
                    error,
                )
                if attempt < max_attempts:
                    time.sleep(self.retry_delay_seconds)
        raise RuntimeError("Unable to connect to Kafka") from last_error

    def process_message(self, raw_value: bytes | str) -> bool:
        """Validate and process one message, returning whether it was anomalous."""

        payload = parse_metric(raw_value)
        if payload is None:
            return False

        self.processed_count += 1
        anomaly_detected = process_metric(payload, cpu_threshold=self.cpu_threshold)
        if anomaly_detected:
            self.anomaly_count += 1
        LOGGER.info(
            "Running AIOps totals metrics_processed=%d anomalies_detected=%d",
            self.processed_count,
            self.anomaly_count,
        )
        return anomaly_detected

    def report(self) -> None:
        """Log final monitoring totals."""

        LOGGER.info(
            "Total anomalies detected: %d metrics_processed=%d",
            self.anomaly_count,
            self.processed_count,
        )

    def run(self, max_messages: int | None = None) -> int:
        """Run continuously or process a bounded number of valid messages."""

        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive")

        consumer = self.consumer or self.connect_with_retries()
        self.consumer = consumer
        try:
            while not STOP_REQUESTED and (
                max_messages is None or self.processed_count < max_messages
            ):
                try:
                    records = consumer.poll(timeout_ms=1000)
                    for messages in records.values():
                        for message in messages:
                            self.process_message(message.value)
                            if max_messages is not None and self.processed_count >= max_messages:
                                break
                        if max_messages is not None and self.processed_count >= max_messages:
                            break
                except KafkaError:
                    LOGGER.exception("Kafka polling failed; retrying")
                    time.sleep(self.retry_delay_seconds)
        except KeyboardInterrupt:
            LOGGER.info("AIOps monitor shutdown requested")
        finally:
            consumer.close()
            self.report()
        return self.anomaly_count


def stop_monitor(_signum: int, _frame: Any) -> None:
    """Request a clean monitor shutdown."""

    global STOP_REQUESTED
    STOP_REQUESTED = True


if __name__ == "__main__":
    configure_logging()
    signal.signal(signal.SIGINT, stop_monitor)
    signal.signal(signal.SIGTERM, stop_monitor)
    AIOpsMonitor().run()