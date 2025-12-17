import json
import numpy as np
from collections import deque
from config.settings import *
from utils.kafka import create_consumer, create_producer

consumer = create_consumer(KAFKA_BOOTSTRAP, "feature-builder", [TOPIC_OHLCV])
producer = create_producer(KAFKA_BOOTSTRAP)

WINDOW = 30
state = {}

print("Starting feature builder...")

while True:
    msg = consumer.poll(1.0)
    if not msg:
        continue

    bar = json.loads(msg.value())
    sym = bar["symbol"]

    state.setdefault(sym, deque(maxlen=WINDOW)).append(bar)
    if len(state[sym]) < 10:
        continue

    closes = np.array([x["close"] for x in state[sym]])
    volumes = np.array([x["volume"] for x in state[sym]])

    features = {
        "symbol": sym,
        "ts": bar["ts"],
        "log_return": float(np.log(closes[-1] / closes[-2])),
        "volatility": float(np.std(np.diff(np.log(closes)))),
        "volume_z": float((volumes[-1] - volumes.mean()) / (volumes.std() + 1e-6)),
        "range_ratio": (bar["high"] - bar["low"]) / bar["close"],
        "vwap_dev": (bar["close"] - bar["vwap"]) / bar["vwap"]
    }

    producer.produce(TOPIC_FEATURES, json.dumps(features).encode())
    producer.flush()
    print("FEATURES:", features)
