import requests
import redis
import json

SNAPSHOT_URL = "https://api.binance.com/api/v3/depth"
SYMBOL = "BTCUSDT"
LIMIT = 1000  # max depth levels to fetch (5, 10, 20, 50, 100, 500, 1000)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def fetch_snapshot(symbol: str = SYMBOL, limit: int = LIMIT) -> dict:
    response = requests.get(SNAPSHOT_URL, params={
        "symbol": symbol,
        "limit": limit
    })
    response.raise_for_status()
    return response.json()


def store_snapshot(snapshot: dict, symbol: str = SYMBOL):
    last_update_id = snapshot["lastUpdateId"]

    # Clear any existing state for this symbol
    r.delete(f"orderbook:bids:{symbol}")
    r.delete(f"orderbook:asks:{symbol}")
    r.delete(f"orderbook:meta:{symbol}")

    # Store bids in a Redis sorted set
    # score = price (float), member = price string
    # ZSET lets us query top N levels efficiently
    for price, quantity in snapshot["bids"]:
        r.zadd(f"orderbook:bids:{symbol}", {price: float(price)})
        r.hset(f"orderbook:bids:qty:{symbol}", price, quantity)

    # Store asks
    for price, quantity in snapshot["asks"]:
        r.zadd(f"orderbook:asks:{symbol}", {price: float(price)})
        r.hset(f"orderbook:asks:qty:{symbol}", price, quantity)

    # Store metadata
    r.hset(f"orderbook:meta:{symbol}", mapping={
        "last_update_id": last_update_id,
        "symbol": symbol
    })

    print(f"Snapshot loaded — lastUpdateId: {last_update_id}")
    print(f"Bids: {len(snapshot['bids'])} levels")
    print(f"Asks: {len(snapshot['asks'])} levels")


def load_snapshot(symbol: str = SYMBOL):
    snapshot = fetch_snapshot(symbol)
    store_snapshot(snapshot, symbol)
    return snapshot["lastUpdateId"]


if __name__ == "__main__":
    load_snapshot()