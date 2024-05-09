from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("window_exercise").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# create readStream
# read from socket
# localhost:9999
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()
)

# (player1:2342:faults:2).split -> ['player1', '2342', 'faults', '2']

split_lines = lines.withColumn("row", split(col("value"), ":"))

values_count = split_lines.select(
    split_lines.row[0].alias("player"),
    split_lines.row[1].alias("score"),
    split_lines.row[3].alias("faults"),
)

values_count = values_count.groupBy("player").agg(sum(col("score")))


query = values_count.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
