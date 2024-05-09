# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# suppress INFO and WARN log messages
sc.setLogLevel("ERROR")

print("1.")

schema = ["user", "service", "hits"] 
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

# create DataFrame from sample_data and using schema
df = spark.createDataFrame(sample_data, schema)

# print the df 
df.show()

print()
print()
print("2.")

# create a view, which will allow to use SQL on the df
df = df.createOrReplaceTempView("sample_data")

# SQL
selected_df = spark.sql("""select service, hits from sample_data""")

selected_df.show()

“
print()
print()
print("3. ")

# SQL

filtered_df = selected_df.createOrReplaceTempView("filtered_data")
spark.sql("""select * from filtered_data where service="Books" and hits > 3000""").show()


print()
print()
print("4.")

# SQL count(*)
spark.sql("""select count(*) as count from sample_data where service="Books" and hits > 3000 """).show()

