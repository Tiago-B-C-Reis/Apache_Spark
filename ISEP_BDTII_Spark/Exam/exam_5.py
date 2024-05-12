# Imports
from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
from pyspark.sql.functions import *
import os

# NOTE: Adapt to your spark-sql-kafka library version if necessary
kafka_maven = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
checkpoint_location = f"{os.getcwd()}/spark-streaming-eval/"

kafka_broker = "localhost:9092"
kafka_topic = "sensor-data"

# Create SparkSession
spark = (
   SparkSession.builder.master("local")
   .appName("Sensor Data")
   .config("spark.executor.memory", "1g")
   .config("spark.driver.memory", "1g")
   .config("spark.cores.max", "1")
   .config("spark.sql.shuffle.partitions", "1")
   .config("spark.jars.packages", kafka_maven)
   .getOrCreate()
)
# Define schema
schema = (
   StructType()  # type: ignore
   .add("stime", TimestampType())  # type: ignore
   .add("sensor", StringType())  # type: ignore
   .add("data", DoubleType())  # type: ignore
)

# Using Kafka as Source for Structured Streaming
sensor_stream = (
   spark.readStream.format("kafka")
   .option("kafka.bootstrap.servers", kafka_broker)
   .option("startingOffsets", "earliest")
   .option("subscribe", kafka_topic)
   .load()
   .select(from_json(col("value").cast("string"), schema).alias("sensor_data")) # Digest the input JSON
)

# Create the projection
sensor_stream = sensor_stream.select(
   col("sensor_data.stime").alias("stime"),
   col("sensor_data.sensor").alias("sensor"),
   col("sensor_data.data").alias("data"),
)

# Aggregate per sensor value, computing the average, minimum and maximum data for each sensor
sensor_stream = sensor_stream.groupBy(
    sensor_stream.sensor
).agg(
    avg("data").alias("avg_data"),
    min("data").alias("min_data"),
    max("data").alias("max_data")
)

# Start running the query that prints the running counts to the console
query = (
   sensor_stream.writeStream.outputMode("complete")
   .option("truncate", "false")
   .format("console")
   .option("checkpointLocation", checkpoint_location)
   .queryName("Sensor Data")
   .start()
)

# Wait for the stream to be externally stopped
print(f"Ready to read from Kafka ({kafka_broker})")
query.awaitTermination()


