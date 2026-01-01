import psycopg2
import pandas as pd
import numpy as np

DB_CONN = "host=localhost dbname=stockdb user=postgres password=Zener$45"

FEATURES = ["price", "volume", "change"]

def load_data(symbol, limit=2000):
    conn = psycopg2.connect(DB_CONN)
    query = f"""
        SELECT timestamp, price, volume, change, injected
        FROM stock_prices
        WHERE symbol = %s
        ORDER BY timestamp ASC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(symbol, limit))
    conn.close()
    return df

def create_sequences(data, window=20):
    xs = []
    for i in range(len(data) - window):
        xs.append(data[i:i+window])
    return np.array(xs)
