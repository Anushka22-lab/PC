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

## Question 3: Kafka Consumer

Run the consumer in a separate terminal. It validates incoming JSON, logs each
valid metric, and emits an alert when CPU usage is above 80%:

```bash
python q3_kafka_consumer.py
```

The consumer safely skips malformed records and retries temporary Kafka errors.
Set `CPU_ALERT_THRESHOLD` to change the alert threshold or
`KAFKA_CONSUMER_GROUP` to use a different consumer group.

## Question 4: Airflow AIOps Workflow

Install the dependencies and place the DAG where Airflow can discover it:

```bash
python -m pip install -r requirements.txt
cp q4_airflow_aiops_dag.py "$AIRFLOW_HOME/dags/q4_airflow_aiops_dag.py"
airflow dags list | grep aiops_monitoring_workflow
```

The DAG runs `collect_metrics >> process_metrics >> detect_anomaly >>
generate_report` every five minutes, retries failed tasks twice by default,
and uses XCom to pass metrics between tasks. Set `AIRFLOW_TASK_RETRIES`,
`AIRFLOW_RETRY_DELAY_MINUTES`, and `CPU_ALERT_THRESHOLD` to configure it.