from confluent_kafka import Consumer, Producer
import json

# -------------------------------
# Config
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
ALLOWED_LATENESS_MS = 10000

# -------------------------------
# Init
# -------------------------------

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe([INPUT_TOPIC])

# -------------------------------
# State
# -------------------------------

open_candles = {}
closed_candles = {}
max_event_time = 0

# -------------------------------
# Helpers
# -------------------------------

def get_window_start(ts):
    return (ts // WINDOW_SIZE_MS) * WINDOW_SIZE_MS


def is_window_closed(window):
    return max_event_time > window + WINDOW_SIZE_MS + ALLOWED_LATENESS_MS


def create_new_candle(price, quantity):
    return {
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": quantity,
        "version": 1
    }


def update_candle(candle, price, quantity):
    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["volume"] += quantity


def emit(symbol, window, candle, event_type):
    output = {
        "symbol": symbol,
        "interval": "1m",
        "start_time": window,
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"],
        "version": candle["version"],
        "event_type": event_type
    }

    producer.produce(OUTPUT_TOPIC, value=json.dumps(output))
    print(f"{event_type.upper()} EMIT: {output}")


# -------------------------------
# Core Logic
# -------------------------------

def process_trade(trade):
    symbol = trade["symbol"]
    price = trade["price"]
    quantity = trade["quantity"]
    trade_time = trade["trade_time"]

    window = get_window_start(trade_time)
    key = (symbol, window)

    # ---------------------------
    # CASE 1: OPEN WINDOW
    # ---------------------------
    if key in open_candles:
        update_candle(open_candles[key], price, quantity)
        return

    # ---------------------------
    # CASE 2: LATE TRADE (CLOSED WINDOW)
    # ---------------------------
    if key in closed_candles:
        candle = closed_candles[key]

        update_candle(candle, price, quantity)
        candle["version"] += 1

        emit(symbol, window, candle, "correction")
        return

    # ---------------------------
    # CASE 3: NEW WINDOW
    # ---------------------------
    open_candles[key] = create_new_candle(price, quantity)


def close_windows():
    global open_candles

    for (symbol, window), candle in list(open_candles.items()):
        if is_window_closed(window):

            emit(symbol, window, candle, "final")

            closed_candles[(symbol, window)] = candle
            del open_candles[(symbol, window)]

    producer.flush()


# -------------------------------
# Main Loop
# -------------------------------

print(" OHLCV Aggregator (with corrections) started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        trade = json.loads(msg.value().decode("utf-8"))

        trade_time = trade["trade_time"]

        # Update watermark proxy
        max_event_time = max(max_event_time, trade_time)

        process_trade(trade)

        close_windows()

        # Debug state
        print(f"""
📊 STATE
Max Event Time: {max_event_time}
Open: {len(open_candles)}
Closed: {len(closed_candles)}
""")

except KeyboardInterrupt:
    print("Stopping aggregator...")

finally:
    consumer.close()
    producer.flush()