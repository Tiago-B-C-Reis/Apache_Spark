# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Configs
brokers = "localhost:9092"
topic = "json-source"

# Create SparkSession ...
# Define schema of json
schema = StructType() \
    .add("greeting", StringType()) \
    .add("day", IntegerType())

# Using Kafka as Source for Structured Streaming
greetings = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", topic) \
    .load() \
    .select(from_json(col("value").cast("string"), schema) \
    .alias("greetings")) \
    .select(col("greetings.greeting").alias("greeting"),col("greetings.day").alias("day"))

# Start running the query
query = greetings \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()