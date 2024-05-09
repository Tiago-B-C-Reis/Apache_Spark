# Imports
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


broker = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers=[broker],
    value_serializer=lambda x: json.dumps (x).encode("ascii"),
)

topic = "json-source"

greetings = [{"greeting": "Hello", "day": 1},
             {"greeting": "Bonjour", "day": 2},
             {"greeting": "Hola", "day": 3},
             {"greeting": "Ciao", "day": 4},
             {"greeting": "Namaste", "day": 5}
             ]

for greeting in greetings:
    future = producer.send(topic, value=greeting)
    try:
        record_metadata = future.get(timeout=10)
        print("Topic (record_metadata. topic)")
        print("partition (record_metadata.partition}")
        print("offset (record_metadata.offset)")
    except KafkaError:
        pass

producer.flush()