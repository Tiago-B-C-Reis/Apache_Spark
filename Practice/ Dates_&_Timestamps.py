from pyspark.sql import SparkSession
from pyspark.sql.functions import (dayofmonth, dayofweek, dayofyear,
                                   hour, month, year, format_number,
                                   date_format)
spark = SparkSession.builder.appName('dates').getOrCreate()

df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
# df.show()
# print(df.head())
# df.select('Date', 'Open').show()


# df.select(dayofmonth(df['Date'])).show()
# df.select(hour(df['Date'])).show()
# df.select(month(df['Date'])).show()

# df.select(year(df['Date'])).show()
newdf = df.withColumn("Year", year(df['Date']))
result = newdf.groupby("Year").mean().select(["Year", "avg(Close)"])
new_result = result.withColumnRenamed("avg(Close)", "Average Closing Price")
new_result.select(['Year', format_number('Average Closing Price', 2).alias("Avg Close")]).show()
