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
# new local collection
collection = [("Java", 20000), ("Python", 100000), 
("Scala", 3000) , (" Python ", 0), (" Python ", 0)]

# Create an RDD
original = sc.parallelize(collection)
print(original.collect())


print()
print()
print("2. ")
keys_rdd = original.keys()
print(keys_rdd.collect())

print()
print()
print("3. ")

# method that, given a key,value pair
# returns True if value different than zero 
# returns False otherwise
def bigger_numbers(x):
    return x[1] != 0

# apply filter
filtered_rdd = original.filter(bigger_numbers)
print(filtered_rdd.collect())

print()
print()
print("4. ")
def filter_rdd(x):
    # if the key is either "Scala" or "Python"
    return x[0] in ("Scala", "Python")
     
# apply filter
filtered_rdd = original.filter(filter_rdd)
print(filtered_rdd.collect())
