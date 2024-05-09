from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json


spark = SparkSession.builder \
    .appName("KafkaTopic_to_console") \
        .getOrCreate()


# configs
brokers = "localhost:9092"
topic = "player-score-log"

# Define the schema for JSON messages
schema = StructType([
    StructField("player", StringType()),
    StructField("window", StringType()),
    StructField("score", IntegerType()),
    StructField("faults", IntegerType())
])

# Read messages from Kafka topic
messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()
)

# Parse JSON messages and select required columns
parsed_messages = (
    messages.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )
    .select(
        col("data.player").alias("player"),
        col("data.score").alias("score"),
        col("data.faults").alias("faults")
    )
)

# Aggregate scores and faults by player
aggregated_data = (
    parsed_messages.groupBy("player")
    .agg(sum("score").alias("total_score"), sum("faults").alias("total_faults"))
)

# Write aggregated data to the console
query = (
    aggregated_data.writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

print("Ready to read from Kafka topic:", brokers)
query.awaitTermination()