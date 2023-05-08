from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import findspark
findspark.init('/home/tiagor/spark-3.3.2-bin-hadoop3')
spark = SparkSession.builder.appName('exercises').getOrCreate()


df = spark.read.csv('gameclicks.csv', inferSchema=True, header=True)

# View Spark DataFrame schema and count rows:
df.printSchema()

# count the number of rows in the DataFrame:
print("\nNumber of rows in the DataFrame: {}\n".format(df.count()))

# View contents of DataFrame. Call the show() method to view the contents of the DataFrame:
df.show(5)

# Filter columns in DataFrame:
df.select("userId", "teamLevel").show(5)

# Filter rows based on criteria:
df.filter(df["teamLevel"] > 1).select("userId", "teamLevel").show(5)

# Group by a column and count:
df.groupBy("isHit").count().show()

# Calculate average and sum:
df.select(mean("isHit"), sum("isHit")).show()


# Add a second dataframe:
df_2 = spark.read.csv('adclicks.csv', inferSchema=True, header=True)
df_2.printSchema()
df_2.show(5)

# Merge df with df_2:
merge_df = df.join(df_2, "userid")
merge_df.printSchema()
merge_df.show(5)


