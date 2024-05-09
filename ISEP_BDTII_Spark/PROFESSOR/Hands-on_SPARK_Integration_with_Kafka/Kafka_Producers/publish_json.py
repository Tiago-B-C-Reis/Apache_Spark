# Imports
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Create producer
broker = "localhost:9092"
topic = "json-source"

producer = KafkaProducer(
    bootstrap_servers=[broker], value_serializer=lambda m: json.dumps(m).encode("ascii")
)

# Example data
messages = [
    {"greeting": "Hello", "day": 1},
    {"greeting": "Bonjour", "day": 2},
    {"greeting": "Hola", "day": 3},
]

# Produce each message as a separate JSON object
for message in messages:
    future = producer.send(topic, value=message)

    # If you want to wait for a message to be delivered, then
    # block and wait (for 'synchronous' sends)
    try:
        record_metadata = future.get(timeout=10)

        # Successful result returns assigned partition and offset
        print(f"Topic {record_metadata.topic}")
        print(f"Partition {record_metadata.partition}")
        print(f"Published offset {record_metadata.offset}")
    except KafkaError:
        # Decide what to do if produce request failed...
        # log.exception()
        pass

producer.flush()
