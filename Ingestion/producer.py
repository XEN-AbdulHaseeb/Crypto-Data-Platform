import json # Enables transfer of raw info between different langs and systems
import websocket # Helps us connect to a live web stream (btcusdt in our case)
from confluent_kafka import Producer # Creates a Kafka producer, check readme.md to learn about Kafka

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

def on_message(ws, message): # Executes when websocket recieves a message
    data = json.loads(message)

    """ Binance sends JSON data, this converts it into a Python dict, the dict 
        The dict will be used later for field extraction,normalizations, etc""" 
    
    # Send raw trade event to Redpanda
    p.produce(
        TOPIC,
        value=json.dumps(data),
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