import json
import csv
import os
from confluent_kafka import Consumer
from config import BOOTSTRAP_SERVERS, TOPICS, SINK_DIR

os.makedirs(SINK_DIR, exist_ok=True)
sink_path = os.path.join(SINK_DIR, "demographics_sink.csv")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "demographics-group-1",
    "auto.offset.reset": "latest",
})

consumer.subscribe([TOPICS["demographics"]])
print(f"Group: demographics-group-1 | Topic: {TOPICS['demographics']}")
print(f"Sink: {sink_path}")
print("Waiting for messages... (Ctrl+C to stop)")
print("-" * 60)

sink_file  = open(sink_path, "w", newline="", encoding="utf-8")
csv_writer = None
rows_written = 0

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

        row = {
            "kafka_key":       key,
            "kafka_topic":     msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset":    msg.offset(),
            **value
        }

        if csv_writer is None:
            csv_writer = csv.DictWriter(sink_file, fieldnames=row.keys())
            csv_writer.writeheader()

        csv_writer.writerow(row)
        sink_file.flush()
        rows_written += 1

        print(
            f"[MSG] key={key} | partition={msg.partition()} | "
            f"offset={msg.offset()} | age={value.get('Age')} | "
            f"gender={value.get('Gender')} | rows_written={rows_written}"
        )

except KeyboardInterrupt:
    print(f"\nStopped. Rows written: {rows_written}")
finally:
    consumer.close()
    sink_file.close()
