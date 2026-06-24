from confluent_kafka import Consumer, Producer
import json
import redis
import math

# -------------------------------
# Config
# -------------------------------

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "anomaly-detector",
    "auto.offset.reset": "earliest"
}

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}

INPUT_TOPIC = "ohlcv.btcusdt"
OUTPUT_TOPIC = "anomalies.btcusdt"

WINDOW_SIZE = 20       # number of past pct_changes to track
Z_SCORE_THRESHOLD = 3  # flag anything beyond 3 std devs

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

def changes_key(symbol):
    return f"anomaly:changes:{symbol}"

def prev_close_key(symbol):
    return f"anomaly:prev_close:{symbol}"

# -------------------------------
# Math Helpers
# -------------------------------

def calculate_pct_change(current_close, previous_close):
    return (current_close - previous_close) / previous_close * 100

def calculate_mean(values):
    return sum(values) / len(values)

def calculate_std_dev(values, mean):
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance)

def calculate_z_score(value, mean, std_dev):
    if std_dev == 0:
        return 0  # avoid division by zero during flat/no-volatility periods
    return (value - mean) / std_dev

# -------------------------------
# Rolling Window (Redis List)
# -------------------------------

def get_recent_changes(symbol):
    values = r.lrange(changes_key(symbol), 0, -1)
    return [float(v) for v in values]

def push_change(symbol, pct_change):
    key = changes_key(symbol)
    r.lpush(key, pct_change)
    r.ltrim(key, 0, WINDOW_SIZE - 1)  # keep only the most recent N

# -------------------------------
# Previous Close Tracking
# -------------------------------

def get_previous_close(symbol):
    val = r.get(prev_close_key(symbol))
    return float(val) if val else None

def set_previous_close(symbol, close):
    r.set(prev_close_key(symbol), close)

# -------------------------------
# Emit
# -------------------------------

def emit_anomaly(symbol, candle, pct_change, z_score, mean, std_dev):
    output = {
        "symbol": symbol,
        "start_time": candle["start_time"],
        "close": candle["close"],
        "pct_change": round(pct_change, 4),
        "z_score": round(z_score, 2),
        "rolling_mean": round(mean, 4),
        "rolling_std_dev": round(std_dev, 4),
        "event_type": "anomaly"
    }

    producer.produce(OUTPUT_TOPIC, value=json.dumps(output))
    producer.poll(0)
    print(f"ANOMALY DETECTED — {symbol} z_score={z_score:.2f} pct_change={pct_change:.4f}%")

# -------------------------------
# Main Loop
# -------------------------------

print("Anomaly Detector started...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        candle = json.loads(msg.value().decode("utf-8"))

        # Only evaluate final candles — corrections would distort the rolling window
        if candle.get("event_type") != "final":
            continue

        symbol = candle["symbol"]
        current_close = float(candle["close"])

        previous_close = get_previous_close(symbol)

        if previous_close is None:
            # First candle ever seen for this symbol — nothing to compare yet
            set_previous_close(symbol, current_close)
            continue

        pct_change = calculate_pct_change(current_close, previous_close)

        recent_changes = get_recent_changes(symbol)

        # Need enough history before z-scores are meaningful
        if len(recent_changes) >= 5:
            mean = calculate_mean(recent_changes)
            std_dev = calculate_std_dev(recent_changes, mean)
            z_score = calculate_z_score(pct_change, mean, std_dev)

            if abs(z_score) > Z_SCORE_THRESHOLD:
                emit_anomaly(symbol, candle, pct_change, z_score, mean, std_dev)

        # Update state for next iteration
        push_change(symbol, pct_change)
        set_previous_close(symbol, current_close)

except KeyboardInterrupt:
    print("Stopping anomaly detector...")

finally:
    consumer.close()
    producer.flush()