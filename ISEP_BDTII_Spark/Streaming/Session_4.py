# Imports​
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("DataFrames_exrcises") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName('sessio1').getOrCreate()

# Create readStream
# read from socket
# lines from connection to localhost:9999​
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Create a new DataFrame with the appropriate column names
split_lines = lines.withColumn('row', split(col("value"), ":"))

# DATA TRANSFOMATION: Need to read someting like: "player1:520:faults:2​" to "(player, score, faults)"
values_count = split_lines.select(
    split_lines.row[0].alias("player"),
    split_lines.row[1].alias("score"),
    split_lines.row[2].alias("faults")
    )

# Start running the query that prints the running counts to the console​
query = values_count \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
query.awaitTermination()