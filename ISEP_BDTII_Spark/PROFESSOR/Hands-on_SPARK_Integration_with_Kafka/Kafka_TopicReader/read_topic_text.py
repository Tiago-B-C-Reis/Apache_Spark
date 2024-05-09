# Imports
from kafka import KafkaConsumer

# configs
broker = "localhost:9092"
topic = "text-destination"
group = "cg"
consumer_offset = "earliest"  # or "latest"

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
    topic,
    group_id=group,
    bootstrap_servers=[broker],
    consumer_timeout_ms=5000,  # consume for 5 seconds
    auto_offset_reset=consumer_offset,  # or "latest"
)

# iterate over each message in the consumer
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print(
        "%s:%d:%d: key=%s value=%s"
        % (message.topic, message.partition, message.offset, message.key, message.value)
    )
