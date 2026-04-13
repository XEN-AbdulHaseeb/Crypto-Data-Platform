from confluent_kafka import Consumer
import json

conf = {
    "bootstrap.servers": "localhost:9092", # Redpanda instance
    "group.id": "trade-consumers",
    "auto.offset.reset": "earliest"  # important for first run
}

consumer = Consumer(conf)

TOPIC = "trades.btcusdt"

consumer.subscribe([TOPIC]); """ Delegates partition assignment to our Redpanda broker
                                which select a lead consumer that assigns partitions 
                                to all consumers i.e 'Rebalancing' """

print(" Consumer started...")

try:
    while True:
        msg = consumer.poll(1.0) # Polls broker for a message, 1 second

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode("utf-8")) # De-serialize into python dictionary

        print(f"""  Trade Event
                    Symbol: {data['symbol']}
                    Price: {data['price']}
                    Quantity: {data['quantity']}
                    Trade Time: {data['trade_time']}
                    Ingested At: {data['ingestion_time']}
                    Offset: {msg.offset()}
                    Partition: {msg.partition()}
                """)

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()