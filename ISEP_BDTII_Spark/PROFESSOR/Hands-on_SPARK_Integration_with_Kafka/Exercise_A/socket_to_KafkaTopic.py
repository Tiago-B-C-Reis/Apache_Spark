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

# Create a list
list_lines = split_lines.select(
    split_lines.row[0].alias("player"),
    split_lines.row[1].alias("score"),
    split_lines.row[2].alias("faults"),
    split_lines.timestamp
)

# Create the Window dimension:
window_df = (
    list_lines.groupBy(
        window(
            list_lines.timestamp, # time column
            "20 seconds", # Window duration
            "5 seconds" # Slide duration
        ),
        list_lines.player # select the "player" column from "values_count" table
    ).agg(sum(col("score")), sum(col("faults"))) # order output by window column
)


# Prepare the structure to be sent to the kafka sink topic as JSON
players_Score = window_df.select(
    # Convert to JSON
    to_json(
        # encapsulate the columns you want after processing
        struct(
            col("player").cast(StringType()).alias("player"),
            col("window"),
            col("sum(score)").cast(IntegerType()).alias("score"),
            col("sum(faults)").cast(IntegerType()).alias("faults")
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