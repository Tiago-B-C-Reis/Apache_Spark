from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# # Create a local StreamingContext with two working thread and batch interval of 1 second:
sc = SparkContext('local[2]', 'NetworkWordCount')
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream('localhost', 9999)

# Transform (split) that line into a list of words:
words = lines.flatMap(lambda line: line.split(''))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
word_counts = pairs.reduceByKey(lambda num1, num2: num1+num2)

# Print the first ten elements of each RDD generated in this DStream to the console
word_counts.pprint()

# Now on the terminal, type:
#      $ nc -lk 9999
#      $ hello world any text you want
# With this running, run the line below, then type Ctrl+C to terminate it.
# Start the computation
ssc.start()
# Wait for the computation to terminate
ssc.awaitTermination()
