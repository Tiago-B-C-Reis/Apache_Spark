# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Using Kafka as Source for Structured Streaming
lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", tsource) \
    .load().selectExpr("CAST(value AS STRING)")

# Generate running word count
word_counts = lines \
    .select( explode( split(trim(lines.value), " ") ).alias("word") ) \
    .groupBy("word") \
    .count()

# Write key-value data from a DataFrame to a specific Kafka topic
query = word_counts \
    .selectExpr("CAST(word AS STRING) AS key",
                "concat(CAST(word AS STRING), '=', CAST(count AS STRING)) AS value") \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("topic", tsink) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()
