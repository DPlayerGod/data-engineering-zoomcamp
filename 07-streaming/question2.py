import json
import os
from pathlib import Path
from time import time

import pandas as pd
from kafka import KafkaProducer


def clean_record(record: dict) -> dict:
    cleaned = {}

    for key, value in record.items():
        # NaN / NaT -> None
        if pd.isna(value):
            cleaned[key] = None
        # pandas Timestamp -> string
        elif isinstance(value, pd.Timestamp):
            cleaned[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            cleaned[key] = value

    return cleaned


def json_serializer(message: dict) -> bytes:
    return json.dumps(message, allow_nan=False).encode("utf-8")


BASE_DIR = Path(__file__).resolve().parent
PARQUET_FILE = BASE_DIR / "green_tripdata_2025-10.parquet"
TOPIC = os.getenv("TOPIC_NAME", "green-trips")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=json_serializer,
)

df = pd.read_parquet(PARQUET_FILE)

t0 = time()

for record in df.to_dict(orient="records"):
    cleaned_record = clean_record(record)
    producer.send(TOPIC, value=cleaned_record)

producer.flush()

t1 = time()
print(f"sent {len(df)} records to '{TOPIC}' in {(t1 - t0):.2f} seconds")