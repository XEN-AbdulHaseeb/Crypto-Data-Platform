import json
import time
import websocket
from Ingestion.base_producer import BaseProducer

class TradeProducer(BaseProducer):
    def __init__(self):
        super().__init__(topic="trades.btcusdt")
        self.url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    def transform(self, raw: dict) -> dict:
        return {
            "event_type": "trade",
            "symbol": raw["s"],
            "price": float(raw["p"]),
            "quantity": float(raw["q"]),
            "trade_time": raw["T"],
            "is_buyer_maker": raw["m"],
            "source": "binance",
            "ingestion_time": int(time.time() * 1000)
        }

    def on_message(self, ws, message):
        raw = json.loads(message)
        transformed = self.transform(raw)
        self.send(transformed)

    def on_error(self, ws, error):
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("Connection closed")
        self.flush()

    def on_open(self, ws):
        print("Connected to Binance stream")

    def run(self):
        ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.run_forever()


if __name__ == "__main__":
    TradeProducer().run()