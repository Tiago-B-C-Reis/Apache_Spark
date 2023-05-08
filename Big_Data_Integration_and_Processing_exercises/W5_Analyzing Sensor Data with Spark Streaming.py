from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import re
import logging
# Create a local StreamingContext with two working thread and batch interval of 1 second:
sc = SparkContext(appName="StreamingExample")
ssc = StreamingContext(sc, 1)


# Create a DStream that will connect to hostname:port, like localhost:12028
# Instead of 12028, you may find that port 12020 works instead
lines = ssc.socketTextStream("rtd.hpwren.ucsd.edu", 12028)

# Set the log level to display all messages
logging.basicConfig(level=logging.INFO)

# Parse a line of weather station data, returning the average wind direction measurement
def parse(line):
    match = re.search("Dm=(\d+)", line)
    if match:
        val = match.group(1)
        return [int(val)]
    return []


# Read measurement:
vals = lines.flatMap(parse)

# Create sliding window of data:
window = vals.window(10, 5)


# Define and call analysis function:
def stats(rdd):
    print(rdd.collect())
    if rdd.count() > 0:
        print("max = {}, min = {}".format(rdd.max(), rdd.min()))


# Call the stats() function for each RDD in our sliding window:
window.foreachRDD(lambda rdd: stats(rdd))

# Start the stream processing:
ssc.start()

# Call stop() on the StreamingContext:
ssc.stop()


