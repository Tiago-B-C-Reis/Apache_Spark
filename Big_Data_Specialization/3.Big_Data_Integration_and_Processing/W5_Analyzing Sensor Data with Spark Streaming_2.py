import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import re

# Set the log level to display all messages
logging.basicConfig(level=logging.INFO)

# Create a local StreamingContext with two working threads and a batch interval of 1 second
sc = SparkContext(appName="StreamingExample")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("rtd.hpwren.ucsd.edu", 12020)


# Parse a line of weather station data, returning the average wind direction measurement
def parse(line):
    match = re.search("Dm=(\d+)", line)
    if match:
        val = match.group(1)
        return [int(val)]
    return []


# Read measurement
vals = lines.flatMap(parse)

# Create a sliding window of data
window = vals.window(10, 5)


# Define and call the analysis function
def stats(rdd):
    logging.info("Collected RDD: {}".format(rdd.collect()))
    if rdd.count() > 0:
        logging.info("Max = {}, Min = {}".format(rdd.max(), rdd.min()))


# Call the stats() function for each RDD in our sliding window
window.foreachRDD(lambda rdd: stats(rdd))

# Start the stream processing
ssc.start()

# Wait for the processing to be stopped manually
ssc.awaitTermination()
