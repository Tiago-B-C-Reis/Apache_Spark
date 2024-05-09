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
collection = [1, 2, 3, 4, 5]

# Create an RDD
rdd = sc.parallelize(collection)

# new local collection
collection2 = ("Java", 20000), ("Python", 100000), ("Scala", 3000)

# Create an RDD
rdd2 = sc.parallelize(collection2)
print(rdd.collect())

print()
print()
print("2. ")
print(rdd2.take(2))

# load the file into an RDD
lusiadas_rdd = sc.textFile("lusiadas.txt")


print()
print()
print("4. ")

print(lusiadas_rdd.take(20))

print()
print()
print("5. ")
first_rdd_count = rdd2.count()
print(f"The number of elements in the first RDD is {first_rdd_count}")
lusiadas_rdd_count = lusiadas_rdd.count()
print(f"The number of elements in the Lusiadas' RDD  is {lusiadas_rdd_count}")
