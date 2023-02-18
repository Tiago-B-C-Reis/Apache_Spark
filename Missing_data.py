from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
spark = SparkSession.builder.appName('miss').getOrCreate()

df = spark.read.csv('ContainsNull.csv', inferSchema=True, header=True)
df.show()

# Drop any row with missing data:
df.na.drop().show()

# Drops the column if it has more than 2 nulls
df.na.drop(thresh=2).show()
# Drops if any value of the row has a null:
df.na.drop(how='any').show()
# Drops if all values of the row has a null:
df.na.drop(how='all').show()

# Drops if there is a null on the column 'Sales:
df.na.drop(subset=['Sales']).show()

# Fill in the missing values:
# (if we don't specify, spark function is smart enough to fill
# the empty spaces with the datatype we input)
df.na.fill('Fill value').show()
df.na.fill(0).show()
# Fill missing values on column 'name':
df.na.fill('No Name', subset=['Name']).show()

# Calculates the mean of column 'sales' and store it in a list:
mean_val = df.select(mean(df['Sales'])).collect()
# Collect value mean from list:
mean_sales = mean_val[0][0]
# Fill in:
df.na.fill(mean_sales, ['Sales']).show()
# All in one line:
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0], ['Sales']).show()


