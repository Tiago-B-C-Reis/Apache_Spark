from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("Sink JSON to console").getOrCreate()

sc = spark.sparkContext
# this is simply to restrict the amount of logging sent to the console
# if not set, application would become way too verbose
# sc.setLogLevel("ERROR")

# configs
brokers = "localhost:9092"
topic = "json-source"


# Define schema of json
schema = StructType().add("greeting", StringType()).add("day", IntegerType())

# Using Kafka as Source for Structured Streaming
greetings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("greetings"))
    .select(col("greetings.greeting").alias("greeting"), col("greetings.day").alias("day")
    )
)

# Start running the query
query = greetings.writeStream.outputMode("append").format("console").start()

print(f"Ready to read from Kafka ({brokers})")
query.awaitTermination()
