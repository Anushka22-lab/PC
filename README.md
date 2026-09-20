# AIOps Lab

This project demonstrates an end-to-end server monitoring and anomaly-detection
pipeline. Question 1 is available as a standalone threshold-based detector.

## Question 1: Log Anomaly Detection

Install the Python dependency and run the sample detector:

```bash
python -m pip install -r requirements.txt
python q1_anomaly_detection.py
```

The command prints basic CPU statistics, reports records above 80% CPU usage,
and saves the chart to `q1_anomalies.png`.

## Question 2: Kafka Topic and Producer

Start a local single-node Kafka broker:

```bash
docker compose up -d kafka
python -m pip install -r requirements.txt
```

Create `server_metrics` and publish ten JSON metric messages. The topic setup
is idempotent, and the producer waits for Kafka acknowledgement for each send.

```bash
python q2_kafka_setup.py
python q2_kafka_producer.py
```

Configuration uses environment variables, including `KAFKA_BOOTSTRAP_SERVERS`
(default `localhost:9092`), `KAFKA_TOPIC` (default `server_metrics`),
`KAFKA_MESSAGE_COUNT` (default `10`), and `SERVER_ID` (default `server01`).
Verify published messages with:

```bash
docker compose exec kafka kafka-console-consumer.sh \
	--bootstrap-server localhost:29092 \
	--topic server_metrics --from-beginning --max-messages 10
```