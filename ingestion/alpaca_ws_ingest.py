import json
import random
from collections import defaultdict

from alpaca.data.live import StockDataStream
from kafka import KafkaProducer
from config.settings import (
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY,
    KAFKA_BROKER,
)

# ============================================================
# Kafka Producer
# ============================================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
)

TOPIC = "stock_trades"

# ============================================================
# Injection State (PER SYMBOL)
# ============================================================
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

# ============================================================
# Alpaca Trade Handler
# ============================================================
async def on_trade(trade):
    symbol = trade.symbol
    price = float(trade.price)
    size = float(trade.size)

    injected = should_inject(symbol)

    if injected:
        price *= random.choice([0.6, 1.4])
        size *= random.randint(10, 30)
    else:
        normal_counter[symbol] += 1

    event = {
        "symbol": symbol,
        "price": round(price, 4),
        "size": size,
        "timestamp": trade.timestamp.isoformat(),
        "injected": injected,
        "normal_count": normal_counter[symbol],
        "inject_after": inject_after[symbol],
    }

    producer.send(TOPIC, event)

    # 👇 CLEAR CONSOLE LOGGING
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

# ============================================================
# Main
# ============================================================
def main():
    stream = StockDataStream(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
    )

    stream.subscribe_trades(on_trade, "AAPL", "MSFT", "TSLA")

    print("📡 Alpaca trade stream running")
    print("🧪 Pattern: 5–8 NORMAL → 1 INJECTED → repeat")
    stream.run()

if __name__ == "__main__":
    main()
