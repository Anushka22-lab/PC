"""Airflow workflow for collecting and analyzing server metrics."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)


def collect_metrics() -> dict[str, Any]:
    """Collect one sample server metric and return it through XCom."""

    metrics = {
        "server_id": os.getenv("SERVER_ID", "server01"),
        "cpu_usage": 87,
        "memory_usage": 65,
        "response_time_ms": 420,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    LOGGER.info("Metrics collected metrics=%s", metrics)
    return metrics


def process_metrics(**context: Any) -> dict[str, Any]:
    """Read collected metrics from XCom and validate the required fields."""

    metrics = context["ti"].xcom_pull(task_ids="collect_metrics")
    if not isinstance(metrics, dict):
        raise ValueError("collect_metrics did not return a metric dictionary")

    required_fields = ("server_id", "cpu_usage", "memory_usage", "response_time_ms", "timestamp")
    missing_fields = [field for field in required_fields if field not in metrics]
    if missing_fields:
        raise ValueError(f"Collected metrics missing fields: {missing_fields}")

    LOGGER.info("Metrics processed metrics=%s", metrics)
    return metrics


def detect_anomaly(**context: Any) -> bool:
    """Detect high CPU usage and return the anomaly result through XCom."""

    metrics = context["ti"].xcom_pull(task_ids="process_metrics")
    threshold = float(os.getenv("CPU_ALERT_THRESHOLD", "80"))
    if not isinstance(metrics, dict) or not isinstance(metrics.get("cpu_usage"), (int, float)):
        raise ValueError("Processed metrics contain no valid cpu_usage value")
    if not 0 <= threshold <= 100:
        raise ValueError("CPU_ALERT_THRESHOLD must be between 0 and 100")

    anomaly_detected = metrics["cpu_usage"] > threshold
    if anomaly_detected:
        LOGGER.warning("Anomaly detected: High CPU usage cpu_usage=%s", metrics["cpu_usage"])
    else:
        LOGGER.info("No anomaly detected cpu_usage=%s", metrics["cpu_usage"])
    return anomaly_detected


def generate_report(**context: Any) -> None:
    """Log a final AIOps report from the previous task results."""

    metrics = context["ti"].xcom_pull(task_ids="process_metrics")
    anomaly_detected = context["ti"].xcom_pull(task_ids="detect_anomaly")
    LOGGER.info(
        "AIOps report metrics_collected=%s metrics_processed=%s anomaly_detection_completed=%s "
        "anomaly_detected=%s",
        bool(metrics),
        bool(metrics),
        anomaly_detected is not None,
        anomaly_detected,
    )


default_args = {
    "owner": "aiops",
    "depends_on_past": False,
    "retries": int(os.getenv("AIRFLOW_TASK_RETRIES", "2")),
    "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "1"))),
}

with DAG(
    dag_id="aiops_monitoring_workflow",
    description="Collect, process, detect, and report on server metrics",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["aiops", "monitoring"],
) as dag:
    collect_metrics_task = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics,
    )
    process_metrics_task = PythonOperator(
        task_id="process_metrics",
        python_callable=process_metrics,
    )
    detect_anomaly_task = PythonOperator(
        task_id="detect_anomaly",
        python_callable=detect_anomaly,
    )
    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    collect_metrics_task >> process_metrics_task >> detect_anomaly_task >> generate_report_task