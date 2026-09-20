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