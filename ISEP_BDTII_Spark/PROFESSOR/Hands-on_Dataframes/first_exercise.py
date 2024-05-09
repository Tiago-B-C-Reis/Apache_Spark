# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Access SparkContext from spark session
sc = spark.sparkContext

schema = ["user", "service", "hits"]
sampled_data = sample_data = [
    ("user1", "RPG", 300),
    ("user2", "RPG", 460),
    ("user3", "RPG", 410),
    ("user4", "Books", 3000),
    ("user5", "Books", 3000),
    ("user6", "Books", 3300),
    ("user7", "Books", 3900),
    ("user8", "Fanfic", 3000),
    ("user9", "Fanfic", 2000),
]

df = spark.createDataFrame(sampled_data, schema)


df2 = df.filter((col("service") == "Books") & (col("hits") >= 3000)).show()

df2 = df2.groupBy("service").agg(
    max("hits").alias("max_hits"), avg("hits").alias("avg_hits")
)
df2.show()
