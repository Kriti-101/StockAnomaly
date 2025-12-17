import json
import csv
import os
from kafka import KafkaConsumer
from config.settings import *

os.makedirs("data", exist_ok=True)

consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest"
)

csv_path = "data/raw_history.csv"
file_exists = os.path.exists(csv_path)

with open(csv_path, "a", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["symbol","open","high","low","close","volume","timestamp"]
    )
    if not file_exists:
        writer.writeheader()

    print("🗄️ Collecting historical data...")
    for msg in consumer:
        writer.writerow(msg.value)
        f.flush()
