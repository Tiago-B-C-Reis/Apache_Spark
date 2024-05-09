import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import max as spark_max

spark = (SparkSession \
         .builder \
         .master("local") \
         .appName("Example: Exercises") \
         .config("spark.executor.memory", "512mb") \
         .config("spark.driver.memory", "512mb") \
         .config("spark.cores.max", "2") \
         .getOrCreate()
        )

covid_df = spark.read.csv("/Users/tiagoreis/PycharmProjects/Apache_Spark/ISEP_BDTII_Spark/GroupWork/covid_19.csv.txt",
                          header = True,
                          inferSchema = True)


# Gaps 1 - Find the dataframe schema
covid_df.printSchema()


# Gaps 2 – Convert `date_obs` from string to a date object
covid_df2 = covid_df.withColumn('date_obs',
                                to_date('date_obs', 'M/d/yyyy').alias('date'))
covid_df2.printSchema()
covid_df2.show(5)  # Show the first 5 rows after transformation


# Gaps 3 – Get all distinct countries
infected_countries_df = covid_df2.select('country').distinct()
infected_countries_df.show(5)


# Gaps 4 – Count the number of records from Portugal
covid_pt_num_records = covid_df2.filter('country == "Portugal"').count()
print(covid_pt_num_records)


# Gaps 5 – Find the country with the most confirmed cases
result_df = covid_df2.groupBy('country') \
        .agg(max('recovered').alias('max_recovered')) \
                .sort(col('max_recovered').desc()).head()
print(f"Country with most recovered cases: {result_df[0]} (Max Recovered: {result_df[1]})")


# Gaps 6 – Get the most recent observation date for each country/province
most_recent_df = covid_df2 \
        .groupBy('country', 'province') \
                .agg(max('date_obs').alias('most_recent_date_obs')) \
                        .orderBy('most_recent_date_obs').desc
most_recent_df.show(5, truncate=False)  # Show all columns without truncation