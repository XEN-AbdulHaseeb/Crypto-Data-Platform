from confluent_kafka import Consumer, Producer
import json
import redis

# -------------------------------
# Config
# -------------------------------

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "orderbook-consumer",
    "auto.offset.reset": "earliest"
}

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}

INPUT_TOPIC = "orderbook.updates.btcusdt"
OUTPUT_TOPIC = "orderbook.top"
SYMBOL = "BTCUSDT"
TOP_N = 10  # number of levels to emit

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

def bids_key(symbol):
    return f"orderbook:bids:{symbol}"

def asks_key(symbol):
    return f"orderbook:asks:{symbol}"

def bids_qty_key(symbol):
    return f"orderbook:bids:qty:{symbol}"

def asks_qty_key(symbol):
    return f"orderbook:asks:qty:{symbol}"

def meta_key(symbol):
    return f"orderbook:meta:{symbol}"

# -------------------------------
# Sequence Validation
# -------------------------------

def get_last_update_id(symbol):
    val = r.hget(meta_key(symbol), "last_update_id")
    return int(val) if val else None

def set_last_update_id(symbol, update_id):
    r.hset(meta_key(symbol), "last_update_id", update_id)

def is_valid_first_message(U, u, last_update_id):
    """
    Binance rule for first message after snapshot:
    U <= lastUpdateId + 1 <= u
    """
    return U <= last_update_id + 1 <= u

def is_valid_sequence(U, prev_u):
    """
    Binance rule for all subsequent messages:
    U must equal prev_u + 1
    """
    return U == prev_u + 1

# -------------------------------
# Order Book Update
# -------------------------------

def apply_update(symbol, side, levels):
    """
    Apply a list of [price, quantity] updates to one side of the book.
    quantity == "0" means remove that price level.
    """
    if side == "bids":
        zset_key = bids_key(symbol)
        qty_key = bids_qty_key(symbol)
    else:
        zset_key = asks_key(symbol)
        qty_key = asks_qty_key(symbol)

    for price, quantity in levels:
        if quantity == "0":
            # Remove price level entirely
            r.zrem(zset_key, price)
            r.hdel(qty_key, price)
        else:
            # Add or update price level
            r.zadd(zset_key, {price: float(price)})
            r.hset(qty_key, price, quantity)

# -------------------------------
# Top of Book
# -------------------------------

def get_top_of_book(symbol, n=TOP_N):
    # Top bids = highest prices (ZREVRANGE)
    top_bids = r.zrevrange(bids_key(symbol), 0, n - 1)
    # Top asks = lowest prices (ZRANGE)
    top_asks = r.zrange(asks_key(symbol), 0, n - 1)

    bids_qty_map = r.hgetall(bids_qty_key(symbol))
    asks_qty_map = r.hgetall(asks_qty_key(symbol))

    bids = [[p, bids_qty_map.get(p, "0")] for p in top_bids]
    asks = [[p, asks_qty_map.get(p, "0")] for p in top_asks]

    return bids, asks

# -------------------------------
# Emit
# -------------------------------

def emit_top_of_book(symbol, update_id):
    bids, asks = get_top_of_book(symbol)

    output = {
        "symbol": symbol,
        "last_update_id": update_id,
        "bids": bids,
        "asks": asks
    }

    producer.produce(OUTPUT_TOPIC, value=json.dumps(output))
    producer.poll(0)
    print(f"Emitted top of book — updateId: {update_id}")

# -------------------------------
# Resync
# -------------------------------

def resync(symbol):
    """
    Gap detected — state is corrupt.
    Clear in-memory tracking and reload snapshot.
    """
    print(f"RESYNC triggered for {symbol} — reloading snapshot...")
    from processing.snapshot_loader import load_snapshot
    load_snapshot(symbol)

# -------------------------------
# Main Loop
# -------------------------------

print("Order Book Processor started...")

first_message = True
prev_u = None

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        update = json.loads(msg.value().decode("utf-8"))

        symbol = update["symbol"]
        U = int(update["first_update_id"])
        u = int(update["final_update_id"])
        bids = update["bids"]
        asks = update["asks"]

        last_update_id = get_last_update_id(symbol)

        if last_update_id is None:
            print("No snapshot found — run snapshot_loader first")
            continue

        # -------------------------------
        # Sequence Validation
        # -------------------------------

        if first_message:
            if not is_valid_first_message(U, u, last_update_id):
                # Message is older than snapshot — discard
                print(f"Discarding stale message — u={u}, lastUpdateId={last_update_id}")
                continue

            print(f"First valid message accepted — U={U}, u={u}")
            first_message = False

        else:
            if not is_valid_sequence(U, prev_u):
                print(f"GAP DETECTED — expected U={prev_u + 1}, got U={U}")
                resync(symbol)
                first_message = True
                prev_u = None
                continue

        # -------------------------------
        # Apply Updates
        # -------------------------------

        apply_update(symbol, "bids", bids)
        apply_update(symbol, "asks", asks)

        set_last_update_id(symbol, u)
        prev_u = u

        # Emit top of book after every update
        emit_top_of_book(symbol, u)

except KeyboardInterrupt:
    print("Stopping processor...")

finally:
    consumer.close()
    producer.flush()