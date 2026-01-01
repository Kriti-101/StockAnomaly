import numpy as np
import psycopg2
import joblib
from tensorflow.keras.models import load_model
from utils import load_data, create_sequences, FEATURES

WINDOW = 20
THRESHOLD = 0.015  # tune later

model = load_model("lstm_model.h5")
scaler = joblib.load("scaler.save")

conn = psycopg2.connect(
    "host=localhost dbname=stockdb user=postgres password=Zener45"
)
cur = conn.cursor()

SYMBOLS = ["RELIANCE", "INFY", "TCS"]

for symbol in SYMBOLS:
    df = load_data(symbol)

    if len(df) < WINDOW + 1:
        continue

    scaled = scaler.transform(df[FEATURES])
    X = create_sequences(scaled, WINDOW)

    preds = model.predict(X, verbose=0)
    mse = np.mean(np.abs(preds - X), axis=(1,2))

    df = df.iloc[WINDOW:]
    df["anomaly_score"] = mse

    for _, row in df.iterrows():
        if row["anomaly_score"] > THRESHOLD:
            cur.execute(
                """
                INSERT INTO fraud_events
                (timestamp, symbol, anomaly_score, source)
                VALUES (%s, %s, %s, %s)
                """,
                (row["timestamp"], symbol, float(row["anomaly_score"]), "LSTM")
            )

    conn.commit()
    print(f"🚨 {symbol}: {sum(df.anomaly_score > THRESHOLD)} anomalies")

conn.close()
