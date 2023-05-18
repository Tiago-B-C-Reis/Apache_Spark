from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructField
import findspark
import csv
findspark.init('/home/tiagor/spark-3.3.2-bin-hadoop3')
spark = SparkSession.builder.appName('Spark_W6').getOrCreate()


# ---------------------------------------------------------------------------------------------------------
# Create DataFrame for the twitter csv file.
twitter_df = spark.read.csv('twitter.csv', inferSchema=True, header=True)

# Count the number of lines
num_lines = twitter_df.count()
# Print the number of lines
print("Number of lines in the file: {}".format(num_lines))

# Split each line into words.
words = twitter_df.rdd.flatMap(lambda line: line[0].split(" "))
# Count the number of words
num_words = words.count()
# Print the number of words
print("Number of words in the file: {}".format(num_words))

# Create tuples for each word with an initial count of 1.
tuples = words.map(lambda word: (word, 1))
# Aggregate the counts for each word
counts = tuples.reduceByKey(lambda a, b: (a + b))
# Collect the results and print each word and its count.
# for (word, count) in counts.collect():
#     print("{}: {}".format(word, count))


# Convert the RDD to a list of strings
result = counts.map(lambda x: "{}: {}".format(x[0], x[1])).collect()

# Save the result to a CSV file
with open('twitter_word_count_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Country', 'Count'])  # Write the header row
    for line in result:
        writer.writerow(line.split(": "))

print("\nResult saved to 'twitter_word_count_output.csv'.")
# ---------------------------------------------------------------------------------------------------------


# Read tweets CSV file into RDD of lines
tweet_lines = spark.read.text("tweet_text.csv")
tweet_lines.show(5, truncate=False)
row_count = tweet_lines.count()
print("\nNumber of rows(tweet_lines): {}\n".format(row_count))

# Clean the data: Remove empty tweets using filter()
tweet_lines = tweet_lines.filter(tweet_lines.value.isNotNull())

# Perform WordCount on the cleaned tweet texts.
words = tweet_lines.rdd.flatMap(lambda line: line[0].split(" "))
tuples = words.map(lambda word: (word, 1))
counts = tuples.reduceByKey(lambda a, b: (a + b))

# Create the DataFrame of tweet word counts
tweetDF = spark.createDataFrame(counts, ["country", "count"])
tweetDF.printSchema()
tweetDF_row_count = tweet_lines.count()
print("\nNumber of rows (tweetDF): {}\n".format(tweetDF_row_count))


# Define the schema for the CSV file
schema = StructType([StructField("Country", StringType(), True),
                     StructField("Code", StringType(), True)])

# Create DataFrame for the country-list csv file.
countries_df = spark.read.csv('country-list.csv', schema=schema, header=False)
countries_df.printSchema()
countries_df.show(5)
countries_df.describe().show()


# Join the country and tweet DataFrames (on the appropriate column)
merge_df = tweetDF.join(countries_df, "Country")
merge_df.printSchema()
merge_df.show(5)
merge_df.describe().show()


# QUESTIONS:
# Question 1: number of distinct countries mentioned
print(merge_df.distinct().count())


# Question 2: number of countries mentioned in tweets.
merge_df.select(sum("Count")).show()


# Question 3: What are the three countries with the highest mentioned count.
merge_df.orderBy(desc("Count")).limit(5).show()


# Question 4: How many times was France mentioned in a tweet?
merge_df.where(merge_df["country"] == "France").show()


# Question 5: Which country was mentioned most: Kenya, Wales, or Netherlands?
countries = ["Kenya", "Wales", "Netherlands"]
    # merge_df.where(merge_df["country"].isin(countries)).show()
filtered_df = merge_df.where(merge_df["country"].isin(countries))
most_mentioned_country = filtered_df.orderBy(desc("count")).first()
print("Most mentioned country: {}".format(most_mentioned_country["country"]))
print("Number of mentions: {}".format(most_mentioned_country["count"]))


# Question 6: What is the average number of times a country is mentioned? (Round to the nearest integer)
country_mention_avg = merge_df.select(avg("count")).first()[0]
print("\nAverage number of times a country is mentioned: {}".format(country_mention_avg))
