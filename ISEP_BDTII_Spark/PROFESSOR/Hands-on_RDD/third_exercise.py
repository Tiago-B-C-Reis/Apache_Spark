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

print("1. ")
collection = [("Java", "OO", 200), ("Python", "interpreted", 100000), 
("Scala", "functional", 30000), ("Elixir", "functional", 3000)]

rdd = sc.parallelize(collection)

print(rdd.collect())

print()
print()
print("2.")
# group by the seconds element - ('00', 'functional', 'interpreted')
rdd_group_by = rdd.groupBy(lambda x: x[1])

# map the RDD to new format 
grouped_rdd = rdd_group_by.mapValues(tuple)
print(grouped_rdd.collect())


print()
print()
print("3. ")
# if we look at the previous RDD as a tuple, the value may be a list of tuples of three entries.
# we want to sort by the very last element of the value of the tuple
# to do so, we create a for loop to access each tuple, and sort by sum of all last values
sum_sorted_rdd = grouped_rdd.sortBy(lambda x: (x[0], sum(key[2] for key in x[1])), ascending=False)

print(sum_sorted_rdd.mapValues(list).collect())
