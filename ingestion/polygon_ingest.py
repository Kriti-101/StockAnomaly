import time
import json
import requests
from datetime import date, timedelta
from kafka import KafkaProducer

from config.settings import (
    POLYGON_API_KEY,
    KAFKA_BROKER,
    RAW_TOPIC,
    TICKERS,
    POLL_INTERVAL
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

BASE_URL = "https://api.polygon.io/v2/aggs/ticker"


def fetch_daily_ohlcv(symbol: str):
    today = date.today()
    start = today - timedelta(days=2)

    url = f"{BASE_URL}/{symbol}/range/1/day/{start}/{today}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 10,
        "apiKey": POLYGON_API_KEY
    }

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json().get("results", [])


def run():
    print("📥 Starting Polygon ingestion (FREE tier)...")

    while True:
        for symbol in TICKERS:
            try:
                bars = fetch_daily_ohlcv(symbol)
                for bar in bars:
                    event = {
                        "symbol": symbol,
                        "open": bar["o"],
                        "high": bar["h"],
                        "low": bar["l"],
                        "close": bar["c"],
                        "volume": bar["v"],
                        "timestamp": bar["t"]
                    }
                    producer.send(RAW_TOPIC, event)
                    print("Sent:", event)

            except Exception as e:
                print(f"Error fetching {symbol}:", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
