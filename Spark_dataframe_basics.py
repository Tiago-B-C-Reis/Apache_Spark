from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StructType,
                               IntegerType, StringType)
spark = SparkSession.builder.appName('Basics').getOrCreate()

# df = spark.read.json('people.json')

# df.show()
# df.printSchema()
# df.columns
# df.describe().show()

# We do this if we want to change/inferring the original schema of the data file:
data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]

final_struc = StructType(fields=data_schema)

df = spark.read.json('people.json', schema=final_struc)
# print(df.printSchema())


# print(type(df['age']))
# df.select('age').show()

print(df.head(2))
df.select('*').show()

# copy column: (does not change original data)
df.withColumn('new_age', df['age']).show()
# copy but with change: (does not change original data)
df.withColumn('double_age', df['age']*2).show()

# this function changes the original data:
df.withColumnRenamed('age', 'my_new_age').show()


df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people")
results.show()
new_results = spark.sql("SELECT * FROM people WHERE age=30")
new_results.show()
