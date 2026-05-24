import csv
import json
import os
from confluent_kafka import Producer
from config import PRODUCER_CONFIG, TOPICS, DATA_DIR

def delivery_callback(err, msg):
    if err:
        print(f"[DELIVERY FAILED] key={msg.key()} | error={err}")
    else:
        print(
            f"[DELIVERED] key={msg.key().decode()} | "
            f"topic={msg.topic()} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()}"
        )

producer = Producer(PRODUCER_CONFIG)

topic    = TOPICS["consumption"]
csv_path = os.path.join(DATA_DIR, "consumption.csv")
key_col  = "respondent_id"

print(f"Topic: {topic}")
print(f"Key: {key_col} — datele de consum ale aceluiasi respondent merg in aceeasi partitie")
print(f"Durability: acks=all, rf=3, min.insync.replicas=2")
print("-" * 60)

with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        key   = str(row[key_col])
        value = json.dumps(row).encode("utf-8")

        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_callback
        )
        producer.poll(0)

producer.flush()
print("-" * 60)
print("Producer consumption finished.")
