from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="aiops-monitor",
    value_deserializer=lambda x:
        json.loads(x.decode("utf-8"))
)

anomaly_count = 0

print("AIOps monitoring started...")

for message in consumer:

    data = message.value

    server = data["server_id"]
    cpu = data["cpu_usage"]
    memory = data["memory_usage"]

    print("\nMessage received:")
    print("Server:", server)
    print("CPU:", cpu, "%")
    print("Memory:", memory, "%")

    if cpu > 80:
        print("ALERT: High CPU detected")
        anomaly_count += 1
    else:
        print("Normal")

    print("Total anomalies detected:", anomaly_count)