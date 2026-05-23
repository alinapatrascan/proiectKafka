import json
import os
from datetime import datetime

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "nasdaq_live"
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "nasdaq-live-consumer-group")


def format_timestamp(ts):
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts).isoformat()
    except Exception:
        return ts


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Listening on topic '{TOPIC}'")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))

            print("-" * 80)
            print(f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key}")
            print(f"Symbol: {value.get('symbol')}")
            print(f"Name: {value.get('name')}")
            print(f"Price: {value.get('price')}")
            print(f"Change: {value.get('change')}")
            print(f"Change %: {value.get('changesPercentage')}")
            print(f"Day Low: {value.get('dayLow')}")
            print(f"Day High: {value.get('dayHigh')}")
            print(f"Year Low: {value.get('yearLow')}")
            print(f"Year High: {value.get('yearHigh')}")
            print(f"Volume: {value.get('volume')}")
            print(f"Market Timestamp: {format_timestamp(value.get('timestamp'))}")
            print(f"Data Type: {value.get('data_type')}")
            print(f"Fetched At UTC: {value.get('fetched_at_utc')}")
            print()

            # Optional: komplettes JSON zusätzlich anzeigen
            print(json.dumps(value, indent=2))
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()