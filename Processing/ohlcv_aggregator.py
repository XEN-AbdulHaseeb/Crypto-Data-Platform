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
current_window = None

# -------------------------------
# Helper Functions
# -------------------------------

def get_window_start(timestamp):
    return (timestamp // 60000) * 60000 # Floor division, to the nearest minute


def process_trade(trade):
    symbol = trade["symbol"]
    price = trade["price"]
    quantity = trade["quantity"]
    trade_time = trade["trade_time"]

    window = get_window_start(trade_time)
    key = (symbol, window) # Symbol is included for scalability reasons, in case we add other crypto-currencies aside from BTC

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


def emit_candles(window_to_emit):
    global candles

    for (symbol, window), candle in list(candles.items()):
        if window == window_to_emit:
            output = {
                "symbol": symbol,
                "interval": "1m",
                "start_time": window,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"]
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

        window = process_trade(trade) # Processes trade msgs into minute candles, returns the minute window

        if current_window is None:
            current_window = window

        if window != current_window: # When a minute passes, this will emit the candle then move to the next minute window
            emit_candles(current_window)
            current_window = window

except KeyboardInterrupt:
    print("🛑 Stopping aggregator...")

finally:
    consumer.close()