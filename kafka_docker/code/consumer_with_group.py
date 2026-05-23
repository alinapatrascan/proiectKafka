from confluent_kafka import Consumer
import json
import sys

# Read the consumer group from the command line
# Example: python consumer.py group-a
group_id = sys.argv[1] if len(sys.argv) > 1 else "seminar-group-1"

# Create a Kafka consumer and connect it to the local broker
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": group_id,
    "auto.offset.reset": "earliest"
})

# Subscribe to the Kafka topic "orders"
consumer.subscribe(["orders"])
print(f"Waiting for data ... group.id={group_id}")

try:
    while True:  # Keep reading messages continuously
        msg = consumer.poll(1.0)  # Wait up to 1 second for a new message
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        # -----------------------------
        # 1. RAW KAFKA PAYLOAD
        # -----------------------------
        raw_key = msg.key()      # Kafka key as raw bytes
        raw_value = msg.value()  # Kafka value as raw bytes

        # -----------------------------
        # 2. APPLICATION INTERPRETATION
        # -----------------------------
        key = raw_key.decode("utf-8") if raw_key else None
        value = json.loads(raw_value.decode("utf-8")) if raw_value else None

        # -----------------------------
        # 3. KAFKA METADATA
        # -----------------------------
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        timestamp = msg.timestamp()
        headers = msg.headers()

        # -----------------------------
        # 4. OUTPUT
        # -----------------------------
        print("-" * 80)
        print(f"CONSUMER GROUP: {group_id}")
        print("KAFKA KEY / VALUE")
        print("Raw key:     ", raw_key)
        print("Decoded key: ", key)
        print("Raw value:   ", raw_value)
        print("Parsed value:", value)

        print()
        print("KAFKA METADATA")
        print("Topic:      ", topic)
        print("Partition:  ", partition)
        print("Offset:     ", offset)
        print("Timestamp:  ", timestamp)
        print("Headers:    ", headers)

except KeyboardInterrupt:
    pass  # Stop cleanly when pressing Ctrl+C
finally:
    consumer.close()  # Close the consumer properly and release resources