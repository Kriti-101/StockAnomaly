import psycopg2
import requests, time, random, os
from datetime import datetime, time as dtime
import pytz
from collections import defaultdict

# ---------------- DB ----------------
conn = psycopg2.connect(
    "host=localhost dbname=stockdb user=postgres password=Zener45"
)
cur = conn.cursor()

# ---------------- CONFIG ----------------
headers = {'User-Agent': 'Mozilla/5.0'}
ist = pytz.timezone('Asia/Kolkata')

MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

SYMBOLS = os.getenv("NSE_SYMBOLS", "RELIANCE,INFY,TCS").split(",")
SYMBOLS = [s.strip().upper() for s in SYMBOLS if s.strip()]

print(f"🚀 NSE → Postgres + Anomaly Injection ({', '.join(SYMBOLS)})")

# ---------------- ANOMALY STATE ----------------
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

# ---------------- HELPERS ----------------
def market_is_open(now_ist: datetime) -> bool:
    return MARKET_OPEN <= now_ist.time() <= MARKET_CLOSE

last_sim_prices = {sym: 1500.0 for sym in SYMBOLS}

# ---------------- MAIN LOOP ----------------
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
            real_change = price_info.get('change', 0.0)
            real_vol    = security_info.get('value', 0) or data.get('totalTradedVolume', 0)

            if market_is_open(now_ist) and real_price is not None:
                price  = float(real_price)
                change = float(real_change or 0)
                vol    = int(real_vol or 1)
                source = "REAL"
            else:
                base = real_price or last_sim_prices[symbol]
                step = random.uniform(-0.003, 0.003)
                price = round(base * (1 + step), 2)
                change = price - base
                vol = 0
                source = "SIM"

            # 🔥 ANOMALY INJECTION
            injected = should_inject(symbol)
            if injected:
                price *= random.choice([0.6, 1.4])
                vol *= random.randint(10, 30)
                print(f"🔥 INJECTED {symbol}: price={price:.2f} vol={vol}")
            else:
                normal_counter[symbol] += 1

            last_sim_prices[symbol] = price

            # ---------------- INSERT ----------------
            cur.execute(
                """
                INSERT INTO stock_prices
                (timestamp, symbol, price, volume, change, injected)
                VALUES (NOW(), %s, %s, %s, %s, %s)
                """,
                (symbol, price, vol, change, injected)
            )
            conn.commit()

            print(
                f"{source} {symbol} | price={price:.2f} "
                f"vol={vol} injected={injected}"
            )

        time.sleep(2)

    except Exception as e:
        print("❌ ERROR:", repr(e))
        time.sleep(5)
