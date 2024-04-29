from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("window_exercise").getOrCreate()

sc = spark.sparkContext
# this is simply to restrict the amount of logging sent to the console
# if not set, application would become way too verbose
sc.setLogLevel("ERROR")

# use the socket we created (using nc -lk 9999)
# to read the stream in the socket using address localhost:9999
# also, include the timestamp of each streamed message
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .option("includeTimestamp", "True")
    .load()
)

# split the input line by ":"
split_lines = lines.withColumn("row", split(col("value"), ":"))

# the previous split line is now a list where:
# * the first entry is the player
# * the second entry is the score
# * the third entry is the faults
# also include the timestamp sent by the message
values_count = split_lines.select(
    split_lines.row[0].alias("player"),
    split_lines.row[1].alias("score"),
    split_lines.row[2].alias("faults"),
    split_lines.timestamp,
)

# we want to group by windows of 20 second, per player
# aggregate these keys by calculating the sum of the scores and faults
# we also order by window, to see a clear timeline of the events
#
# if we set the third parameter of the window function to None,
# we will use a tumbling window instead of a sliding window
values_count = (
    values_count.groupBy(
        window(values_count.timestamp, "20 seconds", "5 seconds"), values_count.player
    )
    .agg(sum(col("score")), sum(col("faults")))
    .orderBy("window")
)

# create an output stream to our console in complete output mode
# also experiment with using the append output mode and understand their differences
#
# we want our application to probe for new messages every 5 seconds
query = (
    values_count.writeStream.outputMode("complete")
    .format("console")
    .trigger(processingTime="5 seconds")
    .start()
)

# await for a termination signal to end the stream
query.awaitTermination()
