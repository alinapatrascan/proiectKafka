import json
from confluent_kafka import Consumer
from config import BOOTSTRAP_SERVERS, TOPICS

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "consumption-group-2",
    "auto.offset.reset": "latest",
})

consumer.subscribe([TOPICS["consumption"]])
print("Group: consumption-group-2 | Topic:", TOPICS["consumption"])
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
            f"meals={value.get('Fast_Food_Meals_Per_Week')} | "
            f"calories={value.get('Average_Daily_Calories')}"
        )

except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()
