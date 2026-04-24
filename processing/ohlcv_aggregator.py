from confluent_kafka import Consumer, Producer
import json
import redis

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

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# -------------------------------
# Redis Key Helpers
# -------------------------------

def open_key(symbol, window):
    return f"open:{symbol}:{window}"

def closed_key(symbol, window):
    return f"closed:{symbol}:{window}"

def get_max_event_time():
    val = r.get("meta:max_event_time")
    return int(val) if val else 0

def update_max_event_time(ts):
    current = get_max_event_time()
    if ts > current:
        r.set("meta:max_event_time", ts)

# -------------------------------
# Window Logic
# -------------------------------

def get_window_start(ts):
    return (ts // WINDOW_SIZE_MS) * WINDOW_SIZE_MS

def is_window_closed(window_start: int, max_event_time: int) -> bool:
    return max_event_time > window_start + WINDOW_SIZE_MS + ALLOWED_LATENESS_MS

# -------------------------------
# Candle Logic
# -------------------------------

def create_or_update_open(symbol, window, price, quantity):
    """
    NOTE:
    This function intentionally re-checks key existence even if the caller
    already performed the same check.

    Rationale:
    - Defensive programming (safe for future call sites)
    - Encapsulation of create/update logic
    - Idempotent behavior for streaming systems

    Trade-off:
    - Extra Redis call; may be optimized later.
    """

    key = open_key(symbol, window)

    if not r.exists(key):
        r.hset(key, mapping={
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": quantity,
            "version": 1
        })
    else:
        c = r.hgetall(key)

        high = max(float(c["high"]), price)
        low = min(float(c["low"]), price)
        volume = float(c["volume"]) + quantity

        r.hset(key, mapping={
            "high": high,
            "low": low,
            "close": price,
            "volume": volume
        })

def handle_late_trade(symbol, window, price, quantity):
    key = closed_key(symbol, window)

    if r.exists(key):
        c = r.hgetall(key)

        high = max(float(c["high"]), price)
        low = min(float(c["low"]), price)
        volume = float(c["volume"]) + quantity
        version = int(c["version"]) + 1

        r.hset(key, mapping={
            "high": high,
            "low": low,
            "close": price,
            "volume": volume,
            "version": version
        })

        updated = r.hgetall(key)
        emit(symbol, window, updated, "correction")

# -------------------------------
# Emit
# -------------------------------

def emit(symbol, window, candle, event_type):
    output = {
        "symbol": symbol,
        "interval": "1m",
        "start_time": window,
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
        "volume": float(candle["volume"]),
        "version": int(candle["version"]),
        "event_type": event_type
    }

    producer.produce(OUTPUT_TOPIC, value=json.dumps(output))
    print(f"{event_type.upper()} EMIT: {output}")

# -------------------------------
# Close Windows
# -------------------------------

def close_windows():
    max_event_time = get_max_event_time()
    for key in r.scan_iter("open:*"):
        _, symbol, window = key.split(":")
        window = int(window)

        if is_window_closed(window, max_event_time):
            candle = r.hgetall(key)

            emit(symbol, window, candle, "final")

            r.rename(key, closed_key(symbol, window))

    producer.flush()

# -------------------------------
# Main Loop
# -------------------------------

print("OHLCV Aggregator (Redis-backed) started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        trade = json.loads(msg.value().decode("utf-8"))

        symbol = trade["symbol"]
        price = float(trade["price"])
        quantity = float(trade["quantity"])
        trade_time = int(trade["trade_time"])

        window_start = get_window_start(trade_time)
        window = window_start # For the sake of clarity

        # Update watermark
        update_max_event_time(trade_time)

        # Decide path
        if r.exists(open_key(symbol, window)):
            create_or_update_open(symbol, window, price, quantity)

        elif r.exists(closed_key(symbol, window)):
            handle_late_trade(symbol, window, price, quantity)

        else:
            create_or_update_open(symbol, window, price, quantity)

        close_windows()

except KeyboardInterrupt:
    print("Stopping aggregator...")

finally:
    consumer.close()
    producer.flush()