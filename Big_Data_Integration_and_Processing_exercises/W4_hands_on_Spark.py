import findspark
findspark.init('/home/tiagor/spark-3.3.2-bin-hadoop3')
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('exercises').getOrCreate()


# Read the text file
DataFrame = spark.read.text('words.txt')


# Show the first 5 lines of the text file:
DataFrame.show(5, truncate=False)

# Count the number of lines
num_lines = DataFrame.count()
# Print the number of lines
print("Number of lines in the file: {}".format(num_lines))

# Split each line into words.
words = DataFrame.rdd.flatMap(lambda line: line[0].split(" "))
# Count the number of words
num_words = words.count()
# Print the number of words
print("Number of words in the file: {}".format(num_words))

# Create tuples for each word with an initial count of 1.
tuples = words.map(lambda word: (word, 1))
# Aggregate the counts for each word
counts = tuples.reduceByKey(lambda a, b: (a + b))
# Collect the results and print each word and its count.
for (word, count) in counts.collect():
    print("{}: {}".format(word, count))

# Save the output to a text file.
# The saveAsTextFile() method in Spark saves the RDD to a directory in HDFS or the local file system, where each
# partition of the RDD is saved as a separate file. The number of output files depends on the number of partitions of
# the RDD, which in turn depends on the number of cores/threads available in your Spark cluster. If you have more than
# one partition in your RDD, then you will have multiple output files.
## counts.map(lambda x: "{}: {}".format(x[0], x[1])).saveAsTextFile("words_output.txt")

# To avoid this, you can repartition the RDD to a single partition before saving it to a file. Here's an example:
counts.map(lambda x: "{}: {}".format(x[0], x[1])).coalesce(1).saveAsTextFile("words_output_1.txt")

# The coalesce(1) method reduces the number of partitions to 1, so that the output will be saved as a single file
# named "words_output.txt". Note that using coalesce() can have performance implications, so use it judiciously.
