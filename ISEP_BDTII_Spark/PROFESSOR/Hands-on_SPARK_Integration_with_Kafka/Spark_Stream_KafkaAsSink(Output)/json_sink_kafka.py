from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Create SparkSession
spark = SparkSession.builder.appName("Sink JSON to Kafka Topic").getOrCreate()

# Set the log level
# spark.sparkContext.setLogLevel("ERROR")

# configs
brokers = "localhost:9092"
tsource = "json-source"
tsink = "json-destination"
checkpoint_dir = os.getcwd() + "/checkpoint_json/"

schema = StructType().add("greeting", StringType()).add("day", IntegerType())

# Using Kafka as Source for Structured Streaming
greetings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", tsource)
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("greetings"))
)

# Take a look at the parsed JSON structure
greetings.printSchema()

# Let's "flatten" the structure
greetings = greetings.selectExpr(
    "greetings['greeting'] AS greeting", "greetings['day'] AS day"
)

# Take a look at the parsed JSON structure
greetings.printSchema()

# Prepare the structure to be sent to the kafka sink topic as JSON
greetings = greetings.select(
    # Convert to JSON
    to_json(
        # encapsulate the columns you want after processing
        struct(col("greeting"), col("day"))
        # don’t Forget to name your element “value”
    ).alias("value")
)

# Write key-value data from a DataFrame to a specific Kafka topic
query = (
    greetings.writeStream.outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", tsink)
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

print(f"Ready to read from Kafka ({brokers})")
query.awaitTermination()
