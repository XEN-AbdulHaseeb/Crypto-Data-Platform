from confluent_kafka import Consumer, Producer
import json

# -------------------------------
# Configuration
# -------------------------------

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ohlcv-consumer",
    "auto.offset.reset": "earliest"
}

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}

INPUT_TOPIC = "trades.btcusdt"
OUTPUT_TOPIC = "ohlcv.btcusdt"

WINDOW_SIZE_MS = 60000
ALLOWED_LATENESS_MS = 10000  # 10 seconds


# -------------------------------
# Initialize Consumer & Producer
# -------------------------------

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe([INPUT_TOPIC])

# -------------------------------
# State Store
# -------------------------------

candles = {}
max_event_time = 0

# -------------------------------
# Helper Functions
# -------------------------------

def get_window_start(timestamp):
    return (timestamp // WINDOW_SIZE_MS) * WINDOW_SIZE_MS # Floor division, to the nearest specified Window

def is_late(window_start):
    return max_event_time > window_start + WINDOW_SIZE_MS + ALLOWED_LATENESS_MS



def process_trade(trade):
    symbol = trade["symbol"]
    price = trade["price"]
    quantity = trade["quantity"]
    trade_time = trade["trade_time"]

    window = get_window_start(trade_time)
    key = (symbol, window) # Symbol is included for scalability reasons, in case we add other crypto-currencies aside from BTC

    # Drop or log late data
    if is_late(window):
        print(f"⚠️ Late trade dropped: {trade}")
        return

    if key not in candles: #For the first trade message in a minute window
        candles[key] = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": quantity
        }
    else: # For subsequent messages
        c = candles[key]
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price
        c["volume"] += quantity

    return window


def emit_candles():
    global candles

    for (symbol, window), candle in list(candles.items()):
         if is_late(window):
            output = {
                "symbol": symbol,
                "interval": "1m",
                "start_time": window,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"] # Quantity of BTC
            }

            producer.produce(OUTPUT_TOPIC, value=json.dumps(output))
            print(f"📤 Emitted candle: {output}")

            del candles[(symbol, window)] # Once emitted, it's no longer necessary to keep track of, it's logged at the ohlcv consumer

    producer.flush() # To empty the internal producer queue

# -------------------------------
# Main Loop
# -------------------------------

print("🚀 OHLCV Aggregator started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue

        trade = json.loads(msg.value().decode("utf-8")) #De-serializing into python dictionary

        # Update max event time (watermark proxy)
        trade_time = trade["trade_time"]
        max_event_time = max(max_event_time, trade_time)

        process_trade(trade) # Processes trade msgs into minute candles, returns the minute window

        # Try to emit any completed windows
        emit_candles()

        # Debug visibility (optional but useful)
        print(f"""
                State
                Max Event Time: {max_event_time}
                Open Windows: {len(candles)}
                """)
        
except KeyboardInterrupt:
    print("🛑 Stopping aggregator...")

finally:
    consumer.close()
    producer.flush()