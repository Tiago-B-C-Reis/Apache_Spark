from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.printSchema()
# df.show()
# print(df.head(3)[0])

# df.filter("Close < 500").select('Open', 'Close').show()
# df.filter(df['close'] < 500).select('Volume').show()
df.filter((df['Close'] < 200) & (df['Open'] > 200)).show()

# '&' for 'and' '|' for 'or' and '~' for 'not':
df.filter((df['Close'] < 200) & ~ (df['Open'] > 200)).show()

df.filter(df['Low'] == 197.16).show()
# This one return the same result that the code above but in a list
# format, so it can be stored:
result = df.filter(df['Low'] == 197.16).collect()
print(result)
# Transform it into a dictionary:
row = result[0]
print(row.asDict())
print(row.asDict()['Volume'])

