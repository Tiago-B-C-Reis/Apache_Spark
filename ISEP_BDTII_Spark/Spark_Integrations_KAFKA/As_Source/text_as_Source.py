# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Configs
brokers = "localhost:9092"
topic = "text-source"

lines = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", topic) \
    .load() \
    .selectExpr("CAST(value AS STRING)")

word_counts = lines \
    .select(explode(split(trim(lines.value), " ")).alias("word")) \
    .groupBy("word") \
    .count()


# Start running the query
query = word_counts.writeStream \
    .outputMode("complete").format("console") \
    .trigger(processingTime="2 seconds") \
    .start()

print(f"Ready to read from Kafka ({brokers})")
query.awaitTermination()
