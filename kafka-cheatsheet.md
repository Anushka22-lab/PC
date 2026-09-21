# Apache Kafka Cheatsheet

Version used: **kafka_2.13-4.3.1** (KRaft mode only, no ZooKeeper). Broker: `localhost:9092`.
On Windows use `.\bin\windows\<name>.bat`; on Linux/macOS use `./bin/<name>.sh`.

## 1. Core concepts

| Term | Meaning |
|---|---|
| Broker | A Kafka server. A cluster is many brokers |
| Topic | Named stream of records (like a table/log) |
| Partition | Ordered, append-only slice of a topic. Unit of parallelism |
| Offset | Position of a record inside a partition |
| Producer | Client that writes records |
| Consumer | Client that reads records |
| Consumer group | Consumers sharing a `group_id`; each partition goes to one consumer in the group |
| Replication factor | Copies of each partition across brokers |
| KRaft | Kafka's built-in metadata quorum (replaces ZooKeeper) |
| Key | Same key always goes to the same partition (keeps per-key ordering) |

Ordering is guaranteed only **within a partition**.

## 2. First-time setup (KRaft)

### Linux / macOS
```bash
cd ~/development/aioops/kafka_2.13-4.3.1

# 1. generate a cluster id
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo $KAFKA_CLUSTER_ID

# 2. format the storage (once)
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

# 3. start the broker (foreground)
bin/kafka-server-start.sh config/server.properties

# start in background
bin/kafka-server-start.sh -daemon config/server.properties

# stop
bin/kafka-server-stop.sh
```

### Windows (PowerShell)
```powershell
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-storage.bat random-uuid
.\bin\windows\kafka-storage.bat format --standalone -t <UUID> -c .\config\server.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### If format fails ("already formatted" / cluster id mismatch)
Wipe the old log dir (`log.dirs` in `config/server.properties`, default `/tmp/kraft-combined-logs`, on Windows `C:\tmp\kraft-combined-logs`), then re-run random-uuid and format.
```bash
rm -rf /tmp/kraft-combined-logs                       # Linux
```
```powershell
Remove-Item -Recurse -Force C:\tmp\kraft-combined-logs # Windows
```
This deletes all topics and data.

### Windows: fix for heap/path issues
Edit `bin\windows\kafka-server-start.bat` (`notepad .\bin\windows\kafka-server-start.bat`) and adjust `KAFKA_HEAP_OPTS` (default `-Xmx1G -Xms1G`).

## 3. Topics (`kafka-topics`)

```bash
BS=localhost:9092

# list
bin/kafka-topics.sh --list --bootstrap-server $BS

# create
bin/kafka-topics.sh --create --topic server_metrics --bootstrap-server $BS
bin/kafka-topics.sh --create --topic server_metrics --partitions 3 --replication-factor 1 --bootstrap-server $BS

# describe (partitions, leader, replicas, ISR)
bin/kafka-topics.sh --describe --topic server_metrics --bootstrap-server $BS

# increase partitions (can never decrease)
bin/kafka-topics.sh --alter --topic server_metrics --partitions 6 --bootstrap-server $BS

# delete
bin/kafka-topics.sh --delete --topic server_metrics --bootstrap-server $BS

# only topics with problems
bin/kafka-topics.sh --describe --under-replicated-partitions --bootstrap-server $BS
```

Create with per-topic config:
```bash
bin/kafka-topics.sh --create --topic logs --bootstrap-server $BS \
  --config retention.ms=86400000 --config cleanup.policy=delete
```

## 4. Console producer / consumer

```bash
# producer: type lines, Ctrl+C to quit
bin/kafka-console-producer.sh --topic server_metrics --bootstrap-server $BS

# producer with keys  (input as key:value)
bin/kafka-console-producer.sh --topic server_metrics --bootstrap-server $BS \
  --property parse.key=true --property key.separator=:

# consumer: only new messages
bin/kafka-console-consumer.sh --topic server_metrics --bootstrap-server $BS

# consumer: from the start
bin/kafka-console-consumer.sh --topic server_metrics --bootstrap-server $BS --from-beginning

# show key, partition, offset, timestamp
bin/kafka-console-consumer.sh --topic server_metrics --bootstrap-server $BS --from-beginning \
  --property print.key=true --property print.partition=true \
  --property print.offset=true --property print.timestamp=true

# read one specific partition from an offset
bin/kafka-console-consumer.sh --topic server_metrics --bootstrap-server $BS \
  --partition 0 --offset 5

# stop after N messages
bin/kafka-console-consumer.sh --topic server_metrics --bootstrap-server $BS --from-beginning --max-messages 10

# as part of a group
bin/kafka-console-consumer.sh --topic server_metrics --bootstrap-server $BS --group server-monitor-group
```

## 5. Consumer groups (`kafka-consumer-groups`)

```bash
# list groups
bin/kafka-consumer-groups.sh --list --bootstrap-server $BS

# lag / offsets / members per partition
bin/kafka-consumer-groups.sh --describe --group server-monitor-group --bootstrap-server $BS

# members / state
bin/kafka-consumer-groups.sh --describe --group server-monitor-group --members --state --bootstrap-server $BS

# reset offsets (group must be inactive). Dry run first, then --execute
bin/kafka-consumer-groups.sh --bootstrap-server $BS --group server-monitor-group \
  --topic server_metrics --reset-offsets --to-earliest --dry-run
bin/kafka-consumer-groups.sh --bootstrap-server $BS --group server-monitor-group \
  --topic server_metrics --reset-offsets --to-earliest --execute

# other reset targets:  --to-latest | --to-offset N | --shift-by -5 | --to-datetime 2026-09-20T10:00:00.000
# delete a group
bin/kafka-consumer-groups.sh --delete --group server-monitor-group --bootstrap-server $BS
```

Columns in `--describe`: `CURRENT-OFFSET` (consumed), `LOG-END-OFFSET` (latest), `LAG` = difference.

## 6. Inspecting offsets and configs

```bash
# latest offset per partition (-1 = latest, -2 = earliest)
bin/kafka-get-offsets.sh --bootstrap-server $BS --topic server_metrics --time -1
bin/kafka-get-offsets.sh --bootstrap-server $BS --topic server_metrics --time -2

# topic config
bin/kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name server_metrics
bin/kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name server_metrics \
  --add-config retention.ms=3600000
bin/kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name server_metrics \
  --delete-config retention.ms

# cluster / KRaft quorum status
bin/kafka-metadata-quorum.sh --bootstrap-server $BS describe --status

# dump log segment
bin/kafka-dump-log.sh --files /tmp/kraft-combined-logs/server_metrics-0/00000000000000000000.log --print-data-log

# broker API versions
bin/kafka-broker-api-versions.sh --bootstrap-server $BS
```

## 7. Python client (`kafka-python`)

```bash
pip install kafka-python
```

### Producer (see `practical1/producer.py`)
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",          # 0 | 1 | "all"
    retries=3,
    linger_ms=5,         # batch small messages
)

meta = producer.send("server_metrics", key="server01",
                     value={"server_id": "server01", "cpu_usage": 95}).get(timeout=10)
print(meta.partition, meta.offset)

producer.flush()
producer.close()
```

### Consumer (see `practical1/consumer.py`)
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    group_id="server-monitor-group",
    auto_offset_reset="earliest",     # earliest | latest (used when the group has no committed offset)
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for msg in consumer:
    print(msg.topic, msg.partition, msg.offset, msg.key, msg.value)
```

Manual commit:
```python
consumer = KafkaConsumer("t", group_id="g", enable_auto_commit=False, bootstrap_servers="localhost:9092")
for msg in consumer:
    process(msg)
    consumer.commit()
```

Batch poll:
```python
records = consumer.poll(timeout_ms=1000, max_records=100)   # {TopicPartition: [ConsumerRecord]}
```

Admin client:
```python
from kafka.admin import KafkaAdminClient, NewTopic
admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
admin.create_topics([NewTopic(name="alerts", num_partitions=3, replication_factor=1)])
print(admin.list_topics())
admin.delete_topics(["alerts"])
```

## 8. Important settings

### Producer
| Setting | Meaning |
|---|---|
| `acks` | `0` no wait, `1` leader ack, `all` all in-sync replicas (safest) |
| `retries` | retry count on transient errors |
| `linger_ms` / `batch_size` | batching for throughput |
| `compression_type` | `gzip`, `snappy`, `lz4`, `zstd` |
| `enable_idempotence` | avoids duplicates on retry |

### Consumer
| Setting | Meaning |
|---|---|
| `group_id` | consumer group |
| `auto_offset_reset` | `earliest` / `latest` when no committed offset exists |
| `enable_auto_commit` | commit offsets automatically |
| `max_poll_records` | max records per poll |
| `session_timeout_ms` | time before broker drops an unresponsive consumer |

### Broker / topic (`config/server.properties`)
| Setting | Meaning |
|---|---|
| `process.roles` | `broker,controller` for single-node KRaft |
| `node.id` | unique node id |
| `listeners` / `advertised.listeners` | where the broker listens / what clients are told |
| `log.dirs` | data directory |
| `num.partitions` | default partitions for new topics |
| `log.retention.hours` / `retention.ms` | how long data is kept (default 7 days) |
| `cleanup.policy` | `delete` or `compact` |
| `auto.create.topics.enable` | create topics on first use |

## 9. Delivery semantics
- **At most once**: commit before processing (may lose data).
- **At least once**: process, then commit (may duplicate). Default choice.
- **Exactly once**: idempotent producer + transactions, or idempotent consumer logic.

## 10. Troubleshooting

| Problem | Fix |
|---|---|
| `NoBrokersAvailable` | Broker not running or wrong `bootstrap_servers` / `advertised.listeners` |
| Consumer prints nothing | Set `auto_offset_reset="earliest"` or use a new `group_id`; the group may already be at the end |
| `Unknown topic or partition` | Create the topic first, or enable `auto.create.topics.enable` |
| Format fails / cluster id mismatch | Delete `log.dirs` contents and re-format (section 2) |
| Port 9092 in use | `ss -ltnp \| grep 9092` (Linux), `netstat -ano \| findstr 9092` (Windows) |
| Rebalancing loops | Processing takes longer than `max_poll_interval_ms`; lower `max_poll_records` |
| Reset offsets refused | Stop all consumers in the group first |

## 11. Quick workflow used in practical
```bash
# terminal 1: broker
bin/kafka-server-start.sh config/server.properties
# terminal 2: topic
bin/kafka-topics.sh --create --topic server_metrics --bootstrap-server localhost:9092
# terminal 3: consumer
python practical1/consumer.py
# terminal 4: producer
python practical1/producer.py
```
