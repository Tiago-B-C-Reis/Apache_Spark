# Imports
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *


# Create SparkSession
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("Example: Fetch from RDD") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName('sessio1').getOrCreate()
    
    

### --------------------------------------------- G – Dataframe Operations​​ ---------------------------------------------------------------------------

# EX:1 - Create a DF using the following collection:​
schema = "user", "service", "hits"
sample_data = [  
    ("user1","RPG",300),
    ("user2","RPG",460),
    ("user3","RPG",410),
    ("user4","Books",3000),
    ("user5","Books",3000),
    ("user6","Books",3300),
    ("user7","Books",3900),
    ("user8","Fanfic",3000),
    ("user9","Fanfic",2000)   
]
# Create the Dataframe​
df = spark.createDataFrame(sample_data, schema)
df.show()


# EX:2 - Create a new DF, selecting only the columns: `service` and `hits`​
new_df = df.select(
        col("service"),
        col("hits").alias("super_hits") )
new_df.show()


# EX:3 - From the previous dataframe, filter all Books with more than 3000 hits​
tmp_df = new_df.filter((col("super_hits") >= 3000))
print("hits​ >= 3000: ")
tmp_df.show()

# EX:4 - Count the number of records in the resulting dataset in the previous exercise (3.)​
print("Number of records: ", tmp_df.count())


# EX:5 - From exercise 3., group by the `service` column. Find the maximum and the average amount of `hits` 
grouped2_df = tmp_df.groupBy("service").agg(
                    max("super_hits").alias("max"),
                    avg("super_hits").alias("avg"))

grouped2_df.show()