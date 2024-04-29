# Imports​
from pyspark.sql import SparkSession
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
    .appName('session_5').getOrCreate()
    
sc = spark.sparkContext
# this is simply to restrict the amount of logging sent to the console
# if not set, application would become way too verbose
sc.setLogLevel("ERROR")

# use the socket we created (using nc -lk 9999)
# to read the stream in the socket using address localhost:9999
# also, include the timestamp of each streamed message
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option("includeTimestamp", True) \
    .load()

# Create a new DataFrame with the appropriate column names
split_lines = lines.withColumn('row', split(col("value"), ":"))

# DATA TRANSFOMATION: Need to read someting like: "player1:520:faults:2​" to "(player, score, faults)"
# the previous split line is now a list where:
# * the first entry is the player
# * the second entry is the score
# * the third entry is the faults
# also include the timestamp sent by the message
values_count = split_lines.select(
    split_lines.row[0].alias("player"),
    split_lines.row[1].alias("score"),
    split_lines.row[2].alias("faults"),
    split_lines.timestamp
)

values_count = (
    values_count.groupBy(
        window(
            values_count.timestamp, # time column
            "20 seconds", # Window duration
            "5 seconds" # Slide duration
        ),
        values_count.player # select the "player" column from "values_count" table
    ).agg(sum(col("score")), sum(col("faults"))) # order output by window column
)

# create an output stream to our console in complete output mode
# Also experiment with using the append output mode and understand their differences
#
# We want our application to probe for new messages every 5 seconds
query = (
    values_count.writeStream.outputMode("update") # UPDATE - in order to use this, the "values_count" must not have sortings like "OrderBy"
    .format("console")
    .trigger(processingTime = "5 seconds")
    .start()
)

# Await for a termination signal to end the stream
query.awaitTermination()