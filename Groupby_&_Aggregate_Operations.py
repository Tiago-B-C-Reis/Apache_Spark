from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, avg, stddev
from pyspark.sql.functions import format_number
spark = SparkSession.builder.appName('aggs').getOrCreate()

df = spark.read.csv('sales_info.csv', inferSchema=True, header=True)
df.show()
df.printSchema()

print(df.groupby("Company"))

df.groupby('Company').mean().show()
df.groupby('Company').max().show()
df.groupby('Company').min().show()
df.groupby('Company').count().show()

df.agg({'Sales': 'max'}).show()

group_data = df.groupby('Company')
group_data.agg({'Sales': 'max'}).show()

# Using PySpark Sql functions:
df.select(countDistinct('Sales')).show()
df.select(avg('Sales').alias('Average Sales')).show()

df.select(stddev('Sales')).show()

sales_std = df.select(stddev('Sales').alias('std'))
sales_std.select(format_number('std', 2).alias('rounded std')).show()

df.orderBy("Sales").show()
df.orderBy(df['Sales'].desc()).show()



