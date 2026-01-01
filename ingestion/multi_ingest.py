import psycopg2
import requests, time, random, os
from datetime import datetime, time as dtime
import pytz

conn = psycopg2.connect("host=localhost dbname=stockdb user=postgres password=Zener45")
cur = conn.cursor()

headers = {'User-Agent': 'Mozilla/5.0'}
ist = pytz.timezone('Asia/Kolkata')

# market hours in IST
MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

# ✅ NEW: Get symbols from env var (add to .env: NSE_SYMBOLS=RELIANCE,TCS,INFY,HDFCBANK)
SYMBOLS = os.getenv("NSE_SYMBOLS", "RELIANCE").split(",")
SYMBOLS = [s.strip().upper() for s in SYMBOLS if s.strip()]

print(f"🚀 NSE → Postgres LIVE ({len(SYMBOLS)} symbols: {', '.join(SYMBOLS)})")

last_sim_prices = {sym: 1500.0 for sym in SYMBOLS}  # per symbol simulation

def market_is_open(now_ist: datetime) -> bool:
    t = now_ist.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE

while True:
    try:
        now_ist = datetime.now(ist)
        
        for symbol in SYMBOLS:
            r = requests.get(
                f'https://www.nseindia.com/api/quote-equity?symbol={symbol}',
                headers=headers,
                timeout=10
            )
            data = r.json()

            price_info    = data.get('priceInfo', {})
            security_info = data.get('securityInfo', {})

            real_price  = price_info.get('lastPrice')
            real_change = price_info.get('change')
            real_vol    = security_info.get('value', 0) or data.get('totalTradedVolume', 0)

            if market_is_open(now_ist) and real_price is not None:
                # use real market data
                price  = real_price
                change = real_change
                vol    = real_vol
                source = "REAL"
            else:
                # simulate a small random walk around last known or close price
                base = real_price or last_sim_prices[symbol]
                step = random.uniform(-0.003, 0.003)  # ±0.3%
                price = round(base * (1 + step), 2)
                change = (price - (real_price or base)) if real_price else 0.0
                vol = 0
                source = "SIM"

            last_sim_prices[symbol] = price

            print(f"{source} {symbol}:", price, vol, change)

            cur.execute(
                "INSERT INTO stock_prices (timestamp, symbol, price, volume, change) "
                "VALUES (NOW(), %s, %s, %s, %s)",
                (symbol, price, vol, change)
            )
            conn.commit()
            print(f"✅ INSERTED {symbol} ({source}): {price}")

    except Exception as e:
        print("❌ ERROR:", repr(e))

    time.sleep(2)  # 2s cycle for all symbols
