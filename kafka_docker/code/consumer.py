from confluent_kafka import Consumer
import json

# Create a Kafka consumer and connect it to the local broker
# "auto.offset.reset": Defines where the consumer should start reading only if no committed offset exists yet for this consumer group.
# "earliest" means: start from the oldest retained records in the topic, so previously stored events can be read again.
# This is very useful for demos, first-time runs, replay scenarios, and learning how offsets work.
# If you restart the consumer with the same group.id and Kafka already has a stored offset for that group,
# this setting usually does not override that saved position.
# Alternative: "latest" means the consumer starts at the end of the log when no offset exists yet,
# so it will wait for new incoming events instead of replaying older retained records.
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "seminar-group-1",
    "auto.offset.reset": "earliest"
})


# Subscribe to the Kafka topic "orders"
consumer.subscribe(["orders"])
print("Waiting for data ...")

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
        print("KAFKA KEY / VALUE")
        print("Raw key:    ", raw_key)
        print("Decoded key:", key)
        print("Raw value:  ", raw_value)
        print("Parsed value:", value)

        print()
        print("KAFKA METADATA")
        print("Topic:     ", topic)
        print("Partition: ", partition)
        print("Offset:    ", offset)
        print("Timestamp: ", timestamp)
        print("Headers:   ", headers)

except KeyboardInterrupt:
    pass  # Stop cleanly when pressing Ctrl+C
finally:
    consumer.close()  # Close the consumer properly and release resources