## Kafka setup

In order to run these examples, start by ensuring you have a Kafka container up. If you dont, please run the following command in your terminal (WSL, if using windows):
`docker run -d \
    --name kafka \
    -e ALLOW_PLAINTEXT_LISTENER=yes \
    -e KAFKA_ENABLE_KRAFT=yes \
    -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
    -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CFG_BROKER_ID=1 \
    -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_KRAFT_CLUSTER_ID=r4zt_wrqTRuT7W2NJsB_GA \
    -p 9092:9092 -p 9093:9093 \
    bitnami/kafka:3.3.1 `

To run list available topics, you can do so with the following command:

`docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list`

You will need to create four topics - `text_source`, `text-destination`, `json-source` and `json-destination`. To do so, you can either invoke the kafka API via docker, or refer to previous examples using the `kafka-python` library. For simplicity sake, we will create these topics using the Kafka API. We will create each topic as having 1 partition and replication factor of 1:
`docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --create --topic text-source`
`docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --create --topic text-destination`
`docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --create --topic json-source`
`docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --create --topic json-destination`

## Examples breakdown (KAFKA as a Source)

Now that we have our kafka cluster up and running, and have the required topics created, we can test the Kafka-Spark integration. This will be divided into 4 parts:
1. Using Kafka, we will create a producer that will publish text data to a topic `text-source`. Using Spark Streaming, we will read from that topic, perform a word count, and sink the result to the `console`
2. Using Kafka, we will create a producer that will publish text data to a topic `text-source`. Using Spark Streaming, we will read from that topic, perform a word count, and sink the result to a Kafka topic `text-destination`. This will then be read by a consumer. 
3. Using Kafka, we will create a producer that will publish JSON data to a topic `json-source`. Using Spark Streaming, we will read from that topic, parse the json fields, and sink the result to the `console`
4. Using Kafka, we will create a producer that will publish JSON data to a topic `json-source`. Using Spark Streaming, we will read from that topic, parse the json fields, and sink the result to a Kafka topic `json-destination`. This will then be read by a consumer.



### Read text and sink to console

Open two terminals (WSL if you are using Windows). In one of them, execute the `text_sink_console.py` Spark Streaming app. This will read from the `text-source` topic, perform the word count, and stream the data to your console. To do so, execute the following command:
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 text_sink_console.py`

This will produce a rather verbose output. When you see that the app is slowing down on the output, that is when it has finished setting up, and you can then start publishing to the topic. You can do so by executing the following, *in the other terminal window*:
`python publish_text.py`

You will then be able to see the batchs of the processed data in the spark window.

### Read text and sink to KAFKA

Open two terminals (WSL if you are using Windows). In one of them, execute the `text_sink_kafka.py` Spark Streaming app. This will read from the `text-source` topic, perform the word count, and stream the data to a Kafka topic `text-destionation`. To do so, execute the following command:
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 text_sink_kafka.py`

This will produce a rather verbose output. When you see that the app is slowing down on the output, that is when it has finished setting up, and you can then start publishing to the topic. You can do so by executing the following, *in the other terminal window*:
`python publish_text.py`

The output will eventually become more stable, and that is when the data has been streamed and submitted to the `text-destination` topic. To validate this, you can run the following script that will read from that topic:
`python read_topic_text.py`


### Read JSON and sink to console

Open two terminals (WSL if you are using Windows). In one of them, execute the `json_sink_console.py` Spark Streaming app. This will read from the `json-source` topic, perform parse the JSON schema, and stream the data to your console. To do so, execute the following command:
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 json_sink_console.py`

This will produce a rather verbose output. When you see that the app is slowing down on the output, that is when it has finished setting up, and you can then start publishing to the topic. You can do so by executing the following, *in the other terminal window*:
`python publish_json.py`

You will then be able to see the batchs of the processed data in the spark window.

### Read text and sink to kafka

Open two terminals (WSL if you are using Windows). In one of them, execute the `json_sink_kafka.py` Spark Streaming app. This will read from the `json-source` topic, perform the word count, and stream the data to a Kafka topic `json-destionation`. To do so, execute the following command:
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 json_sink_kafka.py`

This will produce a rather verbose output. When you see that the app is slowing down on the output, that is when it has finished setting up, and you can then start publishing to the topic. You can do so by executing the following, *in the other terminal window*:
`python publish_json.py`

The output will eventually become more stable, and that is when the data has been streamed and submitted to the `json-destination` topic. To validate this, you can run the following script that will read from that topic:
`python read_topic_json.py`