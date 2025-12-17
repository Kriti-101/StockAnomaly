import json
import time
import joblib
import numpy as np
from collections import defaultdict, deque

from kafka import KafkaConsumer, KafkaProducer
from config.settings import KAFKA_BROKER

# ============================================================
# Config
# ============================================================
INPUT_TOPIC = "stock_trades"
OUTPUT_TOPIC = "fraud_alerts"

BATCH_SIZE = 100
POLL_TIMEOUT = 0.2
Z_THRESHOLD = 4.0   # FAST + sensitive

# ============================================================
# Kafka
# ============================================================
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="fraud-detector-group",
    value_deserializer=lambda m: json.loads(m.decode()),
    enable_auto_commit=False,
    max_poll_records=500,
    auto_offset_reset="latest",
)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ============================================================
# Rolling State (PER SYMBOL)
# ============================================================
price_window = defaultdict(lambda: deque(maxlen=50))
volume_window = defaultdict(lambda: deque(maxlen=50))

# ============================================================
# Z-score detector (FAST)
# ============================================================
def zscore(x, series):
    if len(series) < 10:
        return 0.0
    mean = np.mean(series)
    std = np.std(series) + 1e-6
    return abs((x - mean) / std)

# ============================================================
# Main Loop
# ============================================================
print("🚨 Fraud detector started (FAST MODE)")

while True:
    records = consumer.poll(
        timeout_ms=int(POLL_TIMEOUT * 1000),
        max_records=500,
    )

    batch = []

    for _, msgs in records.items():
        for msg in msgs:
            batch.append(msg.value)

    if not batch:
        continue

    alerts = []

    for event in batch:
        symbol = event["symbol"]
        price = event["price"]
        size = event["size"]

        pz = zscore(price, price_window[symbol])
        vz = zscore(size, volume_window[symbol])

        price_window[symbol].append(price)
        volume_window[symbol].append(size)

        score = max(pz, vz)

        if score > Z_THRESHOLD:
            alert = {
                "symbol": symbol,
                "score": round(score, 2),
                "price": price,
                "size": size,
                "timestamp": event["timestamp"],
                "injected": event.get("injected", False),
            }
            alerts.append(alert)

    # 🔥 SEND ALERTS (BATCHED)
    for alert in alerts:
        producer.send(OUTPUT_TOPIC, alert)

    consumer.commit()

    if alerts:
        print(f"⚠️ {len(alerts)} fraud alerts (batch={len(batch)})")

    time.sleep(0.01)
