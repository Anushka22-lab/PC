"""Simple threshold-based anomaly detection for server metrics."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, stdev
from typing import Iterable


CPU_ANOMALY_THRESHOLD = 80.0


@dataclass(frozen=True)
class ServerMetric:
    """A single application-server observation."""

    timestamp: str
    cpu_usage: float
    memory_usage: float
    response_time_ms: float


def create_sample_dataset() -> list[ServerMetric]:
    """Return twenty deterministic records, including three CPU anomalies."""

    cpu_values = [
        42, 48, 51, 55, 95, 61, 64, 67, 69, 72,
        63, 97, 59, 62, 66, 70, 74, 92, 57, 60,
    ]
    return [
        ServerMetric(
            timestamp=f"10:{minute:02d}",
            cpu_usage=cpu,
            memory_usage=50 + index,
            response_time_ms=180 + index * 8,
        )
        for index, (minute, cpu) in enumerate(
            zip(range(1, 21), cpu_values, strict=True)
        )
    ]


def calculate_statistics(metrics: Iterable[ServerMetric]) -> dict[str, float]:
    """Calculate basic CPU statistics for the supplied observations."""

    cpu_values = [metric.cpu_usage for metric in metrics]
    if not cpu_values:
        raise ValueError("At least one metric is required")

    return {
        "cpu_average": mean(cpu_values),
        "cpu_minimum": min(cpu_values),
        "cpu_maximum": max(cpu_values),
        "cpu_standard_deviation": stdev(cpu_values) if len(cpu_values) > 1 else 0.0,
    }


def detect_anomalies(
    metrics: Iterable[ServerMetric],
    threshold: float = CPU_ANOMALY_THRESHOLD,
) -> list[ServerMetric]:
    """Return records whose CPU usage is above the configured threshold."""

    if threshold < 0 or threshold > 100:
        raise ValueError("CPU threshold must be between 0 and 100")

    return [metric for metric in metrics if metric.cpu_usage > threshold]


def plot_anomalies(
    metrics: Iterable[ServerMetric],
    anomalies: Iterable[ServerMetric],
    output_path: str = "q1_anomalies.png",
) -> None:
    """Save a CPU chart with anomalous observations highlighted."""

    import matplotlib.pyplot as plt

    metric_list = list(metrics)
    anomaly_timestamps = {metric.timestamp for metric in anomalies}
    timestamps = [metric.timestamp for metric in metric_list]
    cpu_values = [metric.cpu_usage for metric in metric_list]
    anomaly_values = [
        metric.cpu_usage
        if metric.timestamp in anomaly_timestamps
        else None
        for metric in metric_list
    ]

    figure, axis = plt.subplots(figsize=(10, 5))
    axis.plot(timestamps, cpu_values, marker="o", label="CPU usage")
    axis.scatter(
        timestamps,
        anomaly_values,
        color="red",
        label="Anomaly",
        zorder=3,
    )
    axis.axhline(CPU_ANOMALY_THRESHOLD, color="orange", linestyle="--", label="Threshold")
    axis.set_title("Server CPU usage anomalies")
    axis.set_xlabel("Timestamp")
    axis.set_ylabel("CPU usage (%)")
    axis.tick_params(axis="x", rotation=45)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_path)
    plt.close(figure)


def main() -> None:
    """Print the report and save the anomaly visualization."""

    metrics = create_sample_dataset()
    anomalies = detect_anomalies(metrics)
    statistics = calculate_statistics(metrics)

    print(f"Total records: {len(metrics)}")
    print(f"CPU average: {statistics['cpu_average']:.1f}%")
    print(f"CPU range: {statistics['cpu_minimum']:.1f}% - {statistics['cpu_maximum']:.1f}%")
    print(f"Anomalies detected: {len(anomalies)}")
    print("Timestamp       CPU       Status")
    for metric in anomalies:
        print(f"{metric.timestamp:<15}{metric.cpu_usage:>3.0f}%      ANOMALY")

    plot_anomalies(metrics, anomalies)
    print("Chart saved to q1_anomalies.png")


if __name__ == "__main__":
    main()