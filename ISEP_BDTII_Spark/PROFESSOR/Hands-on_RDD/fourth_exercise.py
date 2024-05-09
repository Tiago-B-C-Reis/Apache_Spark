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
print("2. ")
reordered_rdd = rdd.map(lambda x: (x[1], x[0], x[2]))
print(reordered_rdd.collect())


print()
print()
print("3. ")
lower_case_rdd = rdd.map(lambda x: (x[0].lower(), x[1], x[2]))
print(lower_case_rdd.collect())


print()
print()
print("4. ")
string_rdd = rdd.map(lambda x: x[0] + ' is a ' + x[1] + ' programming language')
flatten_rdd = string_rdd.flatMap(lambda x: x.split(' '))
print(string_rdd.collect())
