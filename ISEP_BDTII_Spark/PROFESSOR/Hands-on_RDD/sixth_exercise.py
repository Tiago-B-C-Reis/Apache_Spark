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
rdd = sc.parallelize(collection, 4)
print(rdd.collect())

print()
print()
print("2. ")
# you can check the number of partitions by using the len() method,
# which return the length of the list. In this case, as we have 4 partitions
# it returns 4
print(f"The RDD has {len(rdd.glom().collect())} partitions")

print()
print()
print("3. ")
rdd.saveAsTextFile("languages-pt4.txt")
# you will see that it creates a folder called "languages-pt4.txt"
# inside it you will find 4 files with data, and another one called "_SUCCESS"
# this last file is always created when pyspark writes files to disk


print()
print()
print("4. ")
rdd2 = rdd.repartition(1)
print(rdd2.glom().collect())
rdd2.saveAsTextFile("languages-pt1.txt")
# it will now create only 1 file

print()
print()
print("5. ")
print(rdd.glom().collect())
