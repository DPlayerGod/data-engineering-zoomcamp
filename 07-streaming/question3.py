import json
from kafka import KafkaConsumer

TOPIC = "green-trips"
BOOTSTRAP = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id="green-metrics-v2",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

total_gt_5 = 0
total_seen = 0
idle_rounds = 0

print("Start consuming...")

try:
    while True:
        batches = consumer.poll(timeout_ms=1000, max_records=5000)

        if not batches:
            idle_rounds += 1
            if idle_rounds >= 3:
                break
            continue

        idle_rounds = 0

        for _, messages in batches.items():
            for msg in messages:
                row = msg.value
                total_seen += 1

                try:
                    dist = float(row.get("trip_distance", 0) or 0)
                except (TypeError, ValueError):
                    dist = 0.0

                if dist > 5:
                    total_gt_5 += 1

        print(f"processed={total_seen}, trip_distance>5={total_gt_5}")

finally:
    consumer.close()

print(f"Final: processed={total_seen}, trip_distance>5={total_gt_5}")