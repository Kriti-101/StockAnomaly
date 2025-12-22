# diagnostics + core libs
import json
import time
import logging
import numpy as np
from collections import defaultdict, deque

from kafka import KafkaConsumer, KafkaProducer
import os
import sys

# Ensure project root is on sys.path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from config.settings import KAFKA_BROKER

# ============================================================
# Config
# ============================================================
INPUT_TOPIC = "stock_trades"
OUTPUT_TOPIC = "fraud_alerts"

POLL_TIMEOUT = 0.2
Z_THRESHOLD = 4.0   # sensitive but stable
MIN_WINDOW_SIZE = 15  # wait for stability before calculating z-scores

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
# Z-score detector
# ============================================================
def zscore(x, series):
    """Calculate z-score for a value against a series."""
    if len(series) < MIN_WINDOW_SIZE:   # wait for stability
        return 0.0
    mean = np.mean(series)
    std = np.std(series) + 1e-6  # avoid division by zero
    return abs((x - mean) / std)

# ============================================================
# Main Loop
# ============================================================
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("fraud-detector")

logger.info("🚨 Fraud detector starting")
logger.info("📍 Kafka broker: %s", KAFKA_BROKER)
logger.info("📥 Input topic: %s", INPUT_TOPIC)
logger.info("📤 Output topic: %s", OUTPUT_TOPIC)
logger.info("⚙️  Z-score threshold: %.1f", Z_THRESHOLD)

empty_loops = 0
total_processed = 0
total_alerts = 0

while True:
    try:
        records = consumer.poll(
            timeout_ms=int(POLL_TIMEOUT * 1000),
            max_records=500,
        )
    except Exception as e:
        logger.exception("Error polling Kafka consumer: %s", e)
        time.sleep(1.0)
        continue

    # Collect all messages from poll
    batch = []
    for _, msgs in records.items():
        for msg in msgs:
            batch.append(msg.value)

    # Heartbeat logging when idle
    if not batch:
        empty_loops += 1
        if empty_loops % int(10.0 / max(POLL_TIMEOUT, 0.1)) == 0:
            logger.info(
                "💤 Idle: no messages in last %.1f seconds (processed=%d, alerts=%d)",
                empty_loops * POLL_TIMEOUT,
                total_processed,
                total_alerts
            )
        continue
    
    empty_loops = 0
    alerts = []

    # Process each event in the batch
    for event in batch:
        symbol = event.get("symbol")
        if not symbol:
            continue
        
        # NSE ingestion sends data at top level: price, size
        price = event.get("price")
        size = event.get("size")
        
        # Validate required fields
        if price is None or size is None:
            logger.debug("Skipping event with missing price/size: %s", symbol)
            continue

        # Convert to float
        try:
            price = float(price)
            size = float(size)
        except (ValueError, TypeError):
            logger.debug("Skipping event with invalid price/size types: %s", symbol)
            continue

        # Handle injected test events immediately
        if event.get("injected", False):
            # Update windows so injected value becomes part of history
            price_window[symbol].append(price)
            volume_window[symbol].append(size)

            alert = {
                "symbol": symbol,
                "score": None,
                "price": round(price, 4),
                "size": int(size),
                "pz": None,
                "vz": None,
                "timestamp": event.get("timestamp"),
                "injected": True,
                "reason": "test_injection"
            }
            
            # Send immediately for low-latency test visibility
            try:
                producer.send(OUTPUT_TOPIC, alert)
                producer.flush()
                logger.warning("🔥 INJECTED ALERT: %s | price=%.2f size=%d", 
                             symbol, price, size)
            except Exception:
                logger.exception("Failed sending injected alert to Kafka")
            
            alerts.append(alert)
            total_alerts += 1
            continue  # skip z-score calculation for injected events

        # Calculate z-scores BEFORE adding to window
        pz = zscore(price, price_window[symbol])
        vz = zscore(size, volume_window[symbol])

        # Update windows with new values
        price_window[symbol].append(price)
        volume_window[symbol].append(size)

        # Check if anomaly detected
        score = max(pz, vz)

        if score > Z_THRESHOLD:
            # Determine primary reason for alert
            if pz > vz:
                reason = "price_anomaly"
            else:
                reason = "volume_anomaly"
                
            alert = {
                "symbol": symbol,
                "score": round(score, 2),
                "price": round(price, 4),
                "size": int(size),
                "pz": round(pz, 2),
                "vz": round(vz, 2),
                "timestamp": event.get("timestamp"),
                "injected": False,
                "reason": reason
            }
            alerts.append(alert)
            total_alerts += 1
            
            logger.warning(
                "⚠️  FRAUD ALERT: %s | score=%.2f | price=%.2f (z=%.2f) | size=%d (z=%.2f)",
                symbol, score, price, pz, size, vz
            )

    total_processed += len(batch)

    # Send all alerts in batch
    if alerts:
        for alert in alerts:
            try:
                producer.send(OUTPUT_TOPIC, alert)
            except Exception:
                logger.exception("Failed sending alert to Kafka")
        
        try:
            producer.flush()
        except Exception:
            logger.exception("Failed flushing producer")

        logger.info(
            "📊 Batch complete: %d events processed, %d alerts sent",
            len(batch), len(alerts)
        )

    # Commit offsets
    try:
        consumer.commit()
    except Exception:
        logger.exception("Failed to commit consumer offsets")

    # Small yield to prevent CPU spinning
    time.sleep(0.01)