from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json


spark = SparkSession.builder \
    .appName("exercise_A") \
        .getOrCreate()


# configs
brokers = "localhost:9092"
topic_sink = "player-score-log"
checkpoint_dir = os.getcwd() + "/checkpoint_json/"


# Set up the data input connection: (coming from "socket" = nc -lk 9999)
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", True)
    .load()
)

# Creates an array (with one input per "nc -lk 9999" reading) by splitting the input line by ":"
split_lines = lines.withColumn("row", split(col("value"), ":"))

# Prepare the structure to be sent to the kafka sink topic as JSON
players_Score = split_lines.select(
    # Convert to JSON
    to_json(
        # encapsulate the columns you want after processing
        struct(
            col("row").getItem(0).alias("player"),
            lines.timestamp.alias("window"),
            col("row").getItem(1).cast(IntegerType()).alias("score"),
            col("row").getItem(2).cast(IntegerType()).alias("faults")
        )
    ).alias("value")
)


# Write key-value data from a DataFrame to a specific Kafka topic
query = (
    players_Score.writeStream.outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", topic_sink)
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

print(f"Ready to read from 'nc -lk 9999': ({brokers})")
query.awaitTermination()
