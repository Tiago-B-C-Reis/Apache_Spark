from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json

# Spark Session
spark = SparkSession.builder \
    .appName("KafkaTopic_to_console") \
        .getOrCreate()

# configs
broker = "localhost:9092"
topic = "player-score-log"
checkpoint_dir = os.getcwd() + "/checkpoint_Kafka_Reading/"


# Define the schema for JSON messages
schema = (
StructType()
.add("player", StringType())
.add("window", TimestampType())
.add("score", IntegerType())
.add("faults", IntegerType())
)


# Read from Kafka topic
# Using Kafka as Source for Structured Streaming
values_count = (
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("subscribe", topic) \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("values")) # Digest the input JSON
)

values_count = values_count.selectExpr(
    "values['player'] AS player",
    "values['window'] AS window",
    "values['score'] AS score",
    "values['faults'] AS faults",
)

# If we set the third parameter of the window function to None,
# We will use a tumbling window instead of a sliding window
values_count = values_count.groupBy(
    window(
        values_count.window,  # time column
        "20 seconds", # Window duration
        "5 seconds" # Slide duration
        ), 
    values_count.player
).agg(sum(col("score")), sum(col("faults")))


query = values_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()
    
query.awaitTermination()