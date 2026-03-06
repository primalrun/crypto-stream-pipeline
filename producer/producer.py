"""
Binance WebSocket → Kafka producer.

Subscribes to the combined trade stream for BTCUSDT, ETHUSDT, SOLUSDT and
publishes each trade event as a JSON message to the crypto_trades Kafka topic.
Reconnects automatically on disconnect.
"""

import json
import logging
import os
import time

from confluent_kafka import Producer
import websocket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto_trades")
SYMBOLS = os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")

BINANCE_WS_URL = (
    "wss://stream.binance.us:9443/stream?streams="
    + "/".join(f"{s.lower()}@trade" for s in SYMBOLS)
)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def on_message(ws, message):
    envelope = json.loads(message)
    trade = envelope.get("data", {})
    if trade.get("e") != "trade":
        return

    record = {
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "trade_id": trade["t"],
        "timestamp_ms": trade["T"],
        "is_buyer_maker": trade["m"],
    }
    producer.produce(
        KAFKA_TOPIC,
        key=trade["s"].encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
    )
    producer.poll(0)
    log.info("%-8s  price=%-12.2f  qty=%.6f", record["symbol"], record["price"], record["quantity"])


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket closed: %s %s", close_status_code, close_msg)


def on_open(ws):
    log.info("Connected to Binance stream: %s", BINANCE_WS_URL)


def run():
    log.info("Starting producer — bootstrap=%s  topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            log.error("Connection failed: %s — reconnecting in 5s", exc)
        time.sleep(5)


if __name__ == "__main__":
    run()
