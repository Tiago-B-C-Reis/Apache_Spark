import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
spark = SparkSession.builder.appName('data_exploration').getOrCreate()

df = spark.read.csv('daily_weather.csv', inferSchema=True, header=True)

# Look at data columns and types.
df.select(df.columns).show(5)
# The data type for each column.
df.printSchema()
# Number of columns and rows in the DataFrame.
print("Number of columns and rows in the DataFrame: ", len(df.columns))
# The number of rows in the DataFrame.
print("The number of rows in the DataFrame: ", df.count())

# Print summary statistics.
# Compute the statistical summary
summary_df = df.describe()
# Convert to Pandas DataFrame and display
pd.set_option('display.max_rows', None)  # To display all rows
pd.set_option('display.max_columns', None)  # To display all columns
print("\nStatistics summary:\n", summary_df.toPandas().transpose())

df.describe("air_pressure_9am").show()


# Drop rows with missing values.
df2 = df.na.drop(subset=['air_pressure_9am'])
print("The number of rows in the df2:", df2.count())

# Compute correlation between two columns.
print("correlation between rain accumulation and rain duration: ",
      df2.stat.corr("rain_accumulation_9am", "rain_duration_9am"))


# 1- What is the maximum of the average wind speed measurements at 9am (to 2 decimal places)? (23.56)
df.describe("avg_wind_speed_9am").show()

# 2- How many rows containing rain accumulation at 9am measurements have missing values? (6)
df3 = df.na.drop(subset=['rain_accumulation_9am'])
df3_missing_values = df.count() - df3.count()
print("df3_missing_values = ", df3_missing_values)

# 3- What is the correlation between the relative humidity at 9am and at 3pm
# (to 2 decimal places, and without removing or imputing missing values)? (0,88)
print("Correlation between relative_humidity_9am and relative_humidity_3pm: ",
      df2.stat.corr("relative_humidity_9am", "relative_humidity_3pm"))


# 4- What is the approximate maximum max_wind_direction_9am when the maximum max_wind_speed_9am occurs? (68)
# Filter out the rows where max_wind_speed_9am is not the maximum
max_wind_speed = df.agg(max('max_wind_speed_9am')).collect()[0][0]
df_filtered = df.filter(df.max_wind_speed_9am == max_wind_speed)
# Calculate the approximate maximum max_wind_direction_9am
max_wind_direction_approx = df_filtered \
    .approxQuantile('max_wind_direction_9am', [0.5], 0.01)[0]
# Display the result
print(f"The approximate maximum max_wind_direction_9am when the maximum max_wind_speed_9am occurs is "
      f"{max_wind_direction_approx}.")
