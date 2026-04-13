import json # Enables transfer of raw info between different langs and systems
import websocket # Helps us connect to a live web stream (btcusdt in our case)
from confluent_kafka import Producer # Creates a Kafka producer, check readme.md to learn about Kafka
import time

# Redpanda Kafka config
p = Producer({
    "bootstrap.servers": "localhost:9092" # Informs the producer where the broker is
})

TOPIC = "trades.btcusdt"

def delivery_report(err, msg): # Message acknowledgement
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def transform_trade(data):
    return {
        "event_type": "trade",
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "trade_time": data["T"],
        "is_buyer_maker": data["m"],
        "source": "binance",
        "ingestion_time": int(time.time() * 1000) # See annotation below**
    }

""" Binance sends time T in milliseconds as an Integer
    time.time() returns a float in the format seconds.milliseconds
    TO keep ingestion_time consistent we multiply it by 1000 to make it milliseconds and
    typecast it into int """


def on_message(ws, message):
    raw = json.loads(message)
    transformed = transform_trade(raw)

    p.produce( # Producing a transformed stream instead of raw stream
        TOPIC,
        value=json.dumps(transformed), 
        callback=delivery_report
    )

    p.poll(0)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to Binance stream")

if __name__ == "__main__":
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade" # Our channel/Stream

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()