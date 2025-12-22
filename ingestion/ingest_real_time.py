"""NSE poller → Kafka (poll every 2 seconds).

Mirrors the Alpaca ingestion style: uses `config.settings` for symbols and
Kafka broker, injects synthetic anomalies periodically for testing, and
publishes events to topic `stock_trades`.
"""

import json
import time
import random
import os
from collections import defaultdict
from datetime import datetime

import requests
from kafka import KafkaProducer

import os
import sys

# Ensure project root is on sys.path so `import config.settings` works when
# running this script directly (script lives in `ingestion/`).
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import config.settings as settings

# kafka setup
KAFKA_BROKER = getattr(settings, "KAFKA_BROKER", os.environ.get("KAFKA_BROKER", "localhost:9092"))
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
)

TOPIC = "stock_trades"

# Injection state
normal_counter = defaultdict(int)
inject_after = {}

def should_inject(symbol: str) -> bool:
    if symbol not in inject_after:
        inject_after[symbol] = random.randint(5, 8)

    if normal_counter[symbol] >= inject_after[symbol]:
        normal_counter[symbol] = 0
        inject_after[symbol] = random.randint(5, 8)
        return True

    return False


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def fetch_nse_quote(symbol: str):
    # Primary NSE API endpoint used by site
    url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
    r = requests.get(url, headers=HEADERS, timeout=8)
    r.raise_for_status()
    return r.json()


def publish_event(symbol: str, price: float, size: int, raw: dict):
    injected = should_inject(symbol)

    if injected:
        price *= random.choice([0.6, 1.4])
        size *= random.randint(10, 30)
    else:
        normal_counter[symbol] += 1

    event = {
        "symbol": symbol,
        "price": round(float(price), 4),
        "size": size,
        "timestamp": datetime.utcnow().isoformat(),
        "injected": injected,
        "normal_count": normal_counter[symbol],
        "inject_after": inject_after.get(symbol, None),
        "raw": raw,
    }

    producer.send(TOPIC, event)

    if injected:
        print(
            f"🔥 INJECTED | {symbol} | after {event['inject_after']} normals → "
            f"price={event['price']} size={event['size']}"
        )
    else:
        print(
            f"🟢 NORMAL   | {symbol} | count={event['normal_count']} | "
            f"price={event['price']} size={event['size']}"
        )


def main(poll_interval: float = 2.0):
    # symbols: prefer settings.SYMBOLS, fallback to default list
    symbols = getattr(settings, "FINNHUB_SYMBOLS", None) or getattr(settings, "SYMBOLS", ["RELIANCE", "INFY", "TCS"])

    # maintain simple OHLCV per-symbol if desired
    ohlcv = {s: {"o": None, "h": 0, "l": float("inf"), "v": 0} for s in symbols}

    print("🚀 NSE poller → Kafka (every {:.1f}s)".format(poll_interval))
    while True:
        for sym in symbols:
            try:
                data = fetch_nse_quote(sym)

                last_price = data.get("lastPrice") or data.get("price") or 0.0
                total_vol = data.get("totalTradedVolume") or data.get("totalVolume") or 0

                # update OHLCV
                if ohlcv[sym]["o"] is None:
                    ohlcv[sym]["o"] = last_price
                ohlcv[sym]["h"] = max(ohlcv[sym]["h"], last_price)
                ohlcv[sym]["l"] = min(ohlcv[sym]["l"], last_price)
                ohlcv[sym]["v"] += total_vol
                ohlcv[sym]["c"] = last_price

                publish_event(sym, last_price, int(total_vol or 1), raw=data)

                # reset OHLCV at minute boundary
                if int(time.time()) % 60 == 0:
                    ohlcv[sym] = {"o": None, "h": 0, "l": float("inf"), "v": 0}

            except Exception as e:
                print(f"❌ {sym}: {e}")

            # small per-symbol sleep to avoid hammering
            time.sleep(0.1)

        time.sleep(max(0, poll_interval))


if __name__ == "__main__":
    main(2.0)
