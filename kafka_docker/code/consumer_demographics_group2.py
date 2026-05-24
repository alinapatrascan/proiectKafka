import json
from confluent_kafka import Consumer
from config import BOOTSTRAP_SERVERS, TOPICS

# Al doilea consumer pe acelasi topic, group id diferit.
# Kafka ii livreaza o copie completa independent de group-1.
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "demographics-group-2",
    "auto.offset.reset": "latest",
})

consumer.subscribe([TOPICS["demographics"]])
print("Group: demographics-group-2 | Topic:", TOPICS["demographics"])
print("Waiting for messages... (Ctrl+C to stop)")
print("-" * 60)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        key   = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))

        print(
            f"[GROUP-2] key={key} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()} | "
            f"age={value.get('Age')} | "
            f"gender={value.get('Gender')}"
        )

except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()
