ALPACA_API_KEY = 'PKS7S5WV5WDCIEGOMTYZ52Y6SC'
ALPACA_SECRET_KEY = "C3a91cdpkHzAS1pDDgyE1uzuHr7UaBUdJGt5PJLmncqL"
KAFKA_BROKER = "localhost:9092"
RAW_TOPIC = "stockdata"
ALERT_TOPIC = "fraudalerts"

SYMBOLS = ["AAPL", "MSFT", "TSLA"]

# Finnhub configuration
FINNHUB_API_KEY = "d54kk3pr01qojbifq3mgd54kk3pr01qojbifq3n0"
# List of symbols to subscribe to (can be overridden with FINNHUB_SYMBOLS env var)
# Use Indian NSE tickers in Finnhub format (exchange:TICKER)
FINNHUB_SYMBOLS = ["RELIANCE.NS", "INFY.NS", "TCS.NS"]
