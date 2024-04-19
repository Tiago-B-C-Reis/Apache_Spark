from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession \
            .builder \
            .master("local[2]") \
            .getOrCreate()

# Access SparkContext from spark session
sc = spark.sparkContext
# suppress INFO and WARN log messages
sc.setLogLevel("ERROR")

print('1.')
collection = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = sc.parallelize(collection)

print()
print()
print("2")
total_sum = rdd.reduce(lambda acc, x: ("linguagens", acc + x))
print(total_sum)


# for the last exercise, it return the collection exactly as it was initially defined
# this is due to the fact that the reduceByKey() function groups by values of each key.
# since there are only unique keys, it does not do anything.
