import csv
import json
import time
from kafka import KafkaProducer

from config.settings import KAFKA_BROKER

# ============================================================
# CONFIG
# ============================================================
CSV_PATH = "data/us_stock_realtime_with_anomalies.csv"
TOPIC = "stock_trades"

# Speed control
# 1.0 = real time (1 row per minute)
# 0.2 = 5× faster demo
REPLAY_SPEED = 0.2

# ============================================================
# Kafka Producer
# ============================================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ============================================================
# Main Replay Loop
# ============================================================
def main():
    print("🚀 Starting CSV real-time replay ingestion")
    print(f"📄 Source: {CSV_PATH}")
    print(f"⏩ Replay speed: {REPLAY_SPEED}x")
    print("📡 Sending to Kafka topic:", TOPIC)

    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)

        prev_ts = None

        for row in reader:
            # Convert types
            event = {
            "symbol": row["symbol"],

            # ADD THIS LINE (CRITICAL)
            "price": float(row["close"]),

            # Keep OHLC if you want (harmless)
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),

            "volume": float(row["volume"]),
            "timestamp": int(row["timestamp"]),
            "anomaly": int(row.get("anomaly", 0)),
            "source": "csv_replay",
        }


            # Simulate real-time delay
            if prev_ts is not None:
                delta_ms = event["timestamp"] - prev_ts
                sleep_time = max(delta_ms / 1000.0 * REPLAY_SPEED, 0)
                time.sleep(sleep_time)

            prev_ts = event["timestamp"]

            # Send to Kafka
            producer.send(TOPIC, event)
            producer.flush()

            # Console feedback
            if event["anomaly"]:
                print(
                    f"🔥 ANOMALY | {event['symbol']} | "
                    f"close={event['close']} vol={event['volume']}"
                )
            else:
                print(
                    f"🟢 NORMAL  | {event['symbol']} | "
                    f"close={event['close']} vol={event['volume']}"
                )

    print("✅ CSV replay completed")


if __name__ == "__main__":
    main()
