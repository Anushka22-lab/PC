import pandas as pd
import matplotlib.pyplot as plt

data = {
    "Timestamp": [
        "10:01", "10:02", "10:03", "10:04", "10:05",
        "10:06", "10:07", "10:08", "10:09", "10:10",
        "10:11", "10:12", "10:13", "10:14", "10:15",
        "10:16", "10:17", "10:18", "10:19", "10:20"
    ],

    "CPU": [
        45, 50, 55, 60, 95,
        65, 70, 72, 68, 75,
        60, 97, 65, 70, 73,
        76, 78, 92, 67, 71
    ],

    "Memory": [
        60, 61, 62, 63, 64,
        65, 66, 67, 68, 69,
        70, 71, 72, 73, 74,
        75, 76, 77, 78, 79
    ],

    "Response_Time": [
        100, 110, 120, 130, 400,
        140, 150, 160, 170, 180,
        190, 450, 200, 210, 220,
        230, 240, 420, 250, 260
    ]
}

df = pd.DataFrame(data)

print("Total records:", len(df))

print("\nBasic Statistics:")
print(df[["CPU", "Memory", "Response_Time"]].describe())

df["Status"] = df["CPU"].apply(
    lambda x: "ANOMALY" if x > 80 else "NORMAL"
)

anomalies = df[df["Status"] == "ANOMALY"]

print("\nAnomalies detected:", len(anomalies))
print(anomalies[["Timestamp", "CPU", "Status"]])

plt.plot(df["Timestamp"], df["CPU"], marker="o")
plt.axhline(80, linestyle="--")

plt.xlabel("Timestamp")
plt.ylabel("CPU Usage (%)")
plt.title("CPU Anomaly Detection")

plt.xticks(rotation=45)
plt.show()