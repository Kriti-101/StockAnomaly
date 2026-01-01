import time
import numpy as np
import psycopg2
import joblib
from tensorflow.keras.models import load_model

WINDOW = 20
THRESHOLD = 0.015
SYMBOLS = ["RELIANCE", "INFY", "TCS"]
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    dbname="stockdb",
    user="postgres",
    password="Zener45"
)
cur = conn.cursor()

DB = "host=localhost dbname=stockdb user=postgres password=Zener45"

model = load_model("lstm_model.h5", compile=False)

scaler = joblib.load("scaler.save")

conn = psycopg2.connect(DB)
cur = conn.cursor()

print("🚨 LSTM Fraud Detector running...")

def fetch_latest(symbol):
    cur.execute("""
        SELECT timestamp, price, volume, change
        FROM stock_prices
        WHERE symbol=%s
        ORDER BY timestamp DESC
        LIMIT %s
    """, (symbol, WINDOW))
    rows = cur.fetchall()
    return rows[::-1] if len(rows) == WINDOW else None

while True:
    for symbol in SYMBOLS:
        rows = fetch_latest(symbol)
        if not rows:
            continue

        X = np.array([[r[1], r[2], r[3]] for r in rows])
        X = scaler.transform(X).reshape(1, WINDOW, 3)

        recon = model.predict(X, verbose=0)
        mse = np.mean(np.abs(recon - X))

        if mse > THRESHOLD:
            cur.execute("""
                INSERT INTO fraud_events
                (timestamp, symbol, anomaly_score, source)
                VALUES (NOW(), %s, %s, 'LSTM_REALTIME')
            """, (symbol, float(mse)))
            conn.commit()
            print(f"🔥 FRAUD {symbol} | score={mse:.4f}")

    time.sleep(5)
