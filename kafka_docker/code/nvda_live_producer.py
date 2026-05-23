import json
import time
from datetime import datetime, timezone

import requests     # Import requests to call the external HTTP API
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"                        # Address of the local Kafka broker
TOPIC = "nasdaq_live"                                       # Kafka topic where live market data will be published
API_KEY = "fJjTKrPOIGszsEnzEZ3XWuynuu7dImnK"                # API key for the Financial Modeling Prep service
POLL_INTERVAL_SECONDS = 10                                  # Wait time in seconds between two API requests

FMP_URL = "https://financialmodelingprep.com/stable/quote"  # URL of the external stock quote API
SYMBOL = "NVDA"


# Fetch the latest NVDA quote from the external API
def fetch_nvda_quote():
    params = {
        "symbol": SYMBOL,
        "apikey": API_KEY
    }

    response = requests.get(FMP_URL, params=params, timeout=15)

    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code} for {response.url}\n"
            f"Response: {response.text}"
        )

    # Convert the HTTP response body from JSON text into a Python object
    payload = response.json()

    if not isinstance(payload, list) or not payload:
        raise ValueError(f"No quote returned for symbol {SYMBOL}: {payload}")

    # Take the first quote object from the returned list
    quote = payload[0]

    # Build a simplified event payload for Kafka
    return {
        "symbol": quote.get("symbol"),
        "name": quote.get("name"),
        "price": quote.get("price"),
        "change": quote.get("change"),
        "changesPercentage": quote.get("changesPercentage"),
        "dayLow": quote.get("dayLow"),
        "dayHigh": quote.get("dayHigh"),
        "yearHigh": quote.get("yearHigh"),
        "yearLow": quote.get("yearLow"),
        "volume": quote.get("volume"),
        "timestamp": quote.get("timestamp"),
        "source_url": FMP_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_type": "QUOTE"
    }


# Delivery callback: called by Kafka after the broker accepts or rejects the message
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


# Main producer loop
def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Producing {SYMBOL} data to topic '{TOPIC}' from {FMP_URL}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            quote = fetch_nvda_quote()
            key = quote["symbol"]
            value = json.dumps(quote).encode("utf-8")

            # Send the event to Kafka
            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=value,
                callback=delivery_report
            )

            # Trigger Kafka internal delivery handling without blocking
            producer.poll(0)

            print(
                f"Fetched {quote['symbol']} "
                f"price={quote['price']} "
                f"change={quote['change']} "
                f"change%={quote['changesPercentage']} "
                f"at {quote['fetched_at_utc']}"
            )

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()