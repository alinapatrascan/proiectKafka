from confluent_kafka import Producer
import json
import time

# Create a Kafka producer and connect it to the local broker
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

# Define the target Kafka topic
topic = "orders"

# Create and send order messages
for i in range(1, 16):

    # Build one order as a Python dictionary
    order = {
        "order_id": i,
        "customer_id": f"cust-{i % 3}",
        "amount": 100 + i,
        "timestamp": time.time()
    }

    # Send the order to Kafka
    producer.produce(
        topic,
        key=order["customer_id"],
        value=json.dumps(order).encode("utf-8")
    )

# Make sure all queued messages are actually sent before the program exits
producer.flush()