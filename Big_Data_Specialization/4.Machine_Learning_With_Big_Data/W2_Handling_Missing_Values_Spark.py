import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, col
spark = SparkSession.builder.appName('missing_values').getOrCreate()

df = spark.read.csv('daily_weather.csv', inferSchema=True, header=True)

# Compute the statistical summary
summary_df = df.describe()
# Convert to Pandas DataFrame and display
pd.set_option('display.max_rows', None)  # To display all rows
pd.set_option('display.max_columns', None)  # To display all columns
print("\nStatistics summary:\n", summary_df.toPandas().transpose())

df.describe(["air_temp_9am"]).show()
print("Nº of rows in the original df: {}\n".format(df.count()))

# Remove missing values.
removeALLDF = df.na.drop()
removeALLDF.describe(["air_temp_9am"]).show()

# Impute missing values.
imputeDF = df
for x in imputeDF.columns:
    meanValue = removeALLDF.agg(avg(x)).first()[0]
    print(x, meanValue)
    imputeDF = imputeDF.na.fill(meanValue, [x])

df.describe(["air_temp_9am"]).show()
imputeDF.describe(["air_temp_9am"]).show()


# 1- If we remove all missing values from the data, how many air pressure at 9am measurements
# have values between 911.736 and 914.67? (77)
removeALLDF.createOrReplaceTempView('daily_weather')
new_results = spark.sql("SELECT COUNT(air_pressure_9am) FROM daily_weather "
                        "WHERE air_pressure_9am > 911.736 AND air_pressure_9am < 914.67")
new_results.show()
# or:
count = removeALLDF.filter((col('air_pressure_9am') >= 911.736) & (col('air_pressure_9am') <= 914.67)).count()
print(f"The number of air pressure at 9am measurements between 911.736 and 914.67 is: {count}")


# 2- If we impute the missing values with the minimum value,
# how many air temperature at 9am measurements are less than 42.292? (28)
imputeDF_2 = df
for x in imputeDF_2.columns:
    minValue = removeALLDF.agg(min(x)).first()[0]
    imputeDF_2 = imputeDF_2.na.fill(minValue, [x])

count2 = imputeDF_2.filter(col("air_temp_9am") < 42.292).count()
print(f"The number of air temperature at 9am measurements less than 42.292 is: {count2}")


# 3- How many samples have missing values for air_pressure_9am?
print("\nNº of 'air_pressure_9am' rows on the original df: ", df.select("air_pressure_9am").count())
print("Nº of 'air_pressure_9am' rows on the removeALLDF: ", removeALLDF.select("air_pressure_9am").count())
remove_air_pressure_DF = df.na.drop(subset=["air_pressure_9am"])
print("Nº of 'air_pressure_9am' rows on the remove_air_pressure_DF: ",
      remove_air_pressure_DF.select("air_pressure_9am").count())
print("Nº of samples that have missing values for air_pressure_9am: ",
      df.select("air_pressure_9am").count() - remove_air_pressure_DF.select("air_pressure_9am").count())


# 4- Which column in the weather dataset has the most number of missing values?
# Initialize variables
max_missing_count = 0
column_with_most_missing = ''

# Iterate over each column and count the missing values
for column in df.columns:
    missing_count = df.filter(col(column).isNull()).count()
    if missing_count > max_missing_count:
        max_missing_count = missing_count
        column_with_most_missing = column

# Print the column with the most missing values
print("\nThe column with the most missing values is:", column_with_most_missing)
