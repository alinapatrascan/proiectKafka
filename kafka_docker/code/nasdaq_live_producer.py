import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "nasdaq_live"
API_KEY = "fJjTKrPOIGszsEnzEZ3XWuynuu7dImnK"
POLL_INTERVAL_SECONDS = 10

# Kostenlos nutzbarer EOD-Index-Endpunkt
FMP_URL = "https://financialmodelingprep.com/stable/historical-price-eod/light"
SYMBOL = "^IXIC"


def fetch_nasdaq_quote():
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

    payload = response.json()

    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = payload.get("historical", [])
    else:
        raise ValueError(f"Unexpected API response: {payload}")

    if not rows:
        raise ValueError(f"No historical rows returned for symbol {SYMBOL}")

    latest = rows[0]

    prev_price = None
    if len(rows) > 1:
        prev_price = rows[1].get("price")

    price = latest.get("price")
    change = None
    changes_percentage = None

    if price is not None and prev_price not in (None, 0):
        change = price - prev_price
        changes_percentage = (change / prev_price) * 100

    return {
        "symbol": SYMBOL,
        "name": "NASDAQ Composite",
        "price": price,
        "change": change,
        "changesPercentage": changes_percentage,
        "dayLow": None,
        "dayHigh": None,
        "yearHigh": None,
        "yearLow": None,
        "volume": latest.get("volume"),
        "trading_date": latest.get("date"),
        "source_url": FMP_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_type": "EOD"
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Producing NASDAQ data to topic '{TOPIC}' from {FMP_URL}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            quote = fetch_nasdaq_quote()
            key = quote["symbol"]
            value = json.dumps(quote).encode("utf-8")

            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=value,
                callback=delivery_report
            )
            producer.poll(0)

            print(
                f"Fetched {quote['symbol']} "
                f"price={quote['price']} "
                f"date={quote['trading_date']} "
                f"at {quote['fetched_at_utc']}"
            )

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()