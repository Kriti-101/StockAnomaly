import psycopg2
import requests
import time
import random
import json
from collections import deque, defaultdict
import statistics
from datetime import datetime, time as dtime
import pytz

# DB connection
try:
    conn = psycopg2.connect("host=localhost dbname=stockdb user=postgres password=Zener45")
    cur = conn.cursor()
    print("✅ DB connected")
except Exception as e:
    print("⚠️ DB connection failed:", e)
    conn = None
    cur = None

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.nseindia.com/'
}
ist = pytz.timezone('Asia/Kolkata')

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

last_sim_price = defaultdict(lambda: 2500.0)  # per-symbol base price (default ~RELIANCE)

def market_is_open(now_ist: datetime) -> bool:
    t = now_ist.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE

def fetch_nse_quote(session: requests.Session, symbol: str):
    api_url = f'https://www.nseindia.com/api/quote-equity?symbol={symbol}'
    homepage = 'https://www.nseindia.com/'
    
    try:
        session.get(homepage, timeout=10)
        resp = session.get(api_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, dict) and data.get('priceInfo'):
            return data
    except Exception as e:
        print(f"⚠️ NSE fetch failed: {e}")
    
    return None

def insert_price(symbol, price, vol, change):
    if cur is None or conn is None:
        print(f"(DB skipped) {symbol}: {price} vol:{vol}")
        return False
    
    try:
        cur.execute(
            "INSERT INTO stock_prices (timestamp, symbol, price, volume, change) "
            "VALUES (NOW(), %s, %s, %s, %s)",
            (symbol, price, vol, change)
        )
        conn.commit()
        return True
    except Exception as e:
        print("❌ DB INSERT ERROR:", e)
        return False

def main(symbols=None):
    """Ingest one or more symbols. `symbols` is a list of symbol strings."""
    global last_sim_price
    if symbols is None or len(symbols) == 0:
        symbols = ['RELIANCE']

    session = requests.Session()
    session.headers.update(headers)

    print("🚀 NSE → Postgres LIVE")
    print("📈 REAL data: 9:15-15:30 IST | 🔄 Random: other hours")
    print("🔔 Symbols:", ', '.join(symbols))

    while True:
        try:
            now_ist = datetime.now(ist)
            is_market_open = market_is_open(now_ist)

            for symbol in symbols:
                price = None
                vol = 0
                change = 0.0
                source = "SIM"

                # === REAL MARKET DATA (9:15-15:30 IST) ===
                if is_market_open:
                    data = fetch_nse_quote(session, symbol)
                    if data:
                        price_info = data.get('priceInfo', {})
                        security_info = data.get('securityInfo', {})

                        price = price_info.get('lastPrice')
                        change = price_info.get('change', 0)
                        vol = security_info.get('value', 0) or data.get('totalTradedVolume', 0)

                        if price is not None:
                            source = "REAL"
                            print(f"📈 {symbol} {source} ({now_ist.strftime('%H:%M')}): {price} ₹ | vol:{vol:,.0f} | Δ{change:+.2f}")

                # === SIMULATED DATA (outside market hours) or fallback ===
                if price is None or source == "SIM":
                    # Random walk around per-symbol last price
                    step = random.uniform(-2, 2)
                    last = last_sim_price[symbol]
                    price = round(last + step, 2)
                    change = price - last
                    vol = random.randint(500000, 5000000)
                    source = "SIM"

                    # 5% chance of BIG anomaly (for testing)
                    if random.random() < 0.05:
                        anomaly_jump = random.choice([8, -8, 12, -12])
                        price += anomaly_jump
                        print(f"🔥 SIM ANOMALY {symbol}: +₹{anomaly_jump}!")

                    print(f"🔄 {symbol} {source} ({now_ist.strftime('%H:%M')}): {price} ₹ | vol:{vol:,.0f} | Δ{change:+.2f}")

                last_sim_price[symbol] = price

                # INSERT DATA
                if insert_price(symbol, price, vol, change):
                    print(f"✅ INSERTED {symbol}: {price} | vol:{vol:,.0f}")

                # small pause between symbols to avoid hitting API too fast
                time.sleep(0.5)

            # short pause before next round for all symbols
            time.sleep(1)

        except KeyboardInterrupt:
            print("\n🛑 Stopped by user")
            break
        except Exception as e:
            print(f"❌ ERROR: {e}")
            time.sleep(2)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='NSE → Postgres producer (multi-symbol)')
    parser.add_argument('symbols', nargs='*', help='List of symbols to ingest (e.g. RELIANCE TCS INFY)')
    args = parser.parse_args()

    # default to RELIANCE when no args provided
    symbols = args.symbols if args.symbols else ['RELIANCE']
    main(symbols)
