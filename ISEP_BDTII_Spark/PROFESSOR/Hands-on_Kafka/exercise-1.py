from kafka.admin import NewTopic, KafkaAdminClient
from kafka import KafkaConsumer, KafkaProducer
import json


def topic_exists(broker, new_topic):
    # create a consumer object
    consumer = KafkaConsumer(bootstrap_servers=[broker])

    # for each topic in the list of available topics
    # check if it matches the provided topic
    for topic in consumer.topics():
        if new_topic == topic:
            print(f"{new_topic} exists")
            return True
    return False


def create_topic(broker, topic, admin_client):
    # create a NewTopic object, given the name, 
    # number of partitions and replication factor
    new_topic = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    # if it does not exist, create it
    if not topic_exists(broker, topic):
        print(f"creating {topic}")
        admin_client.create_topics(new_topics=new_topic, validate_only=False)


def publish_data(broker, topic, data):
    # create a Kafka Producer object
    # serialize the value, so that it can interpret JSON
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda x: json.dumps(x).encode("ascii"),
    )
    # publish each json oject
    for row in data:
        producer.send(topic, value=row)
    producer.flush()


def consume_data(broker, topic, consumer_group_id, consumer_offset):
    # create a KafkaConsumer object
    # timeout of 5 seconds
    consumer = KafkaConsumer(
        topic,
        group_id=consumer_group_id,
        bootstrap_servers=[broker],
        consumer_timeout_ms=5000,
        auto_offset_reset=consumer_offset,
    )

    # output each message the consumer read
    for message in consumer:
        print(message.value)


def get_offsets(broker):
    # create a Kafka Admin Client object
    kafka = KafkaAdminClient(bootstrap_servers=[broker])
    # list all consumers
    active_consumers = kafka.list_consumer_groups()
    # for each consumer, get its offset
    for consumer in active_consumers:
        partition_mapping = kafka.list_consumer_group_offsets(group_id=consumer[0])
        for partition in partition_mapping:
            offset = partition_mapping[partition]
            print(f"offset: {offset}")


if __name__ == "__main__":
    """
    Main function, where we declare variables, structure the logic 
    and reference the different methods declared above
    """

    broker = "localhost:9092"
    topic = "players-data"
    admin_client = KafkaAdminClient(bootstrap_servers=[broker])

    create_topic(broker, topic, admin_client)

    data = [
        {"_id": 1, "name": "Player1", "score": 520, "faults": 2},
        {"_id": 2, "name": "Player2", "score": 532, "faults": 0},
        {"_id": 3, "name": "Player3", "score": 498, "faults": 1},
        {"_id": 4, "name": "Player4", "score": 566, "faults": 4},
        {"_id": 5, "name": "Player5", "score": 480, "faults": 0},
    ]

    publish_data(broker, topic, data)

    consumer_group_id = "group_id"
    consumer_offset = "earliest"

    consume_data(broker, topic, consumer_group_id, consumer_offset)

    get_offsets(broker)
