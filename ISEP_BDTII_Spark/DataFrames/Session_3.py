# Imports
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create SparkSession
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("DataFrames_exrcises") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName('sessio1').getOrCreate()
    
    
### --------------------------------------------- H – Manipulating DataFrames​​ ---------------------------------------------------------------------------
# EX:1 - Create a DataFrame using the following collection:
schema = "employee_id", "employee_name", "department", "skills"
sample_data = [
    (101, "John Doe", "Sales", "Python, SQL"),  
    (102, "Jane Smith", "Marketing", "Python"),
    (103, "Bob Johnson", "Engineering", "Java, Scala, Python")
]
df1 = spark.createDataFrame(sample_data, schema)
df1.show()


# EX:2 -  Create a new DataFrame that add two new columns - `first_name` and `last_name`
df2 = df1.withColumn('first_name', split(col("employee_name"), " ")[0])\
         .withColumn('last_name', split(col("employee_name"), " ")[1])
df2.show()


# EX:3 -  Using the previous DataFrame, add a new column `number_skills` that has an integer of how many skills an employee has
df3 = df2.withColumn('number_skills', size(split(col("skills"), ", ")))
df3.show()


# EX:4 -  Using the DataFrame created in 2.
df4 = df2.withColumn('skills', split(col('skills'), ", "))\
         .select(col('employee_id'), col('employee_name'), col('department'), 
                 col('first_name'), col('last_name'), explode(col('skills')).alias('skill'))
df4.show()




### --------------------------------------------- I – SQL over Dataframe​ ---------------------------------------------------------------------------
# EX:5 - Create a temporary view from the following collection:
schema_I = "user", "service", "hits"
sample_data_I = [ 
    ("user1", "RPG",300),
    ("user2", "RPG",460),
    ("user3", "RPG",410),
    ("user4", "Books",3000),
    ("user5", "Books",3000),
    ("user6", "Books",3300),
    ("user7", "Books",3900),
    ("user8", "Fanfic",3000),
    ("user9", "Fanfic",2000)
]
service_usage = spark.createDataFrame(sample_data_I, schema_I)
# Create temporary view over Dataframe​
service_usage.createOrReplaceTempView("service_usage")
service_usage.show()


# EX:6 - Use SQL to create a new DF, selecting only the columns: `service` and `hits`. 

ex6_df = spark.sql("""
SELECT service, hits FROM service_usage
""")
ex6_df.show()


# EX:7 - From the previous dataframe, use SQL to filter all Books with more than 3000 hits
ex7_df = spark.sql("""
SELECT service, hits FROM service_usage WHERE service = 'Books' AND hits >= 3000
""")
ex7_df.show()


# EX:8 - Use SQL to count the number of records in the resulting dataset in the (3)
ex7_df.createOrReplaceTempView('ex7_df')  # Register ex7_df as a temporary view

ex8_df = spark.sql("""
SELECT COUNT(*) as count FROM ex7_df
""")
ex8_df.show()





### --------------------------------------------- J – Cleaning datasets​ ---------------------------------------------------------------------------
# EX:9 - Create a dataframe from the following collection:
schema_J = ["first_name", "last_name", "address", "city", "phone_number"] 
data_J = [ 
  ["Josh", "Grant", "Rua Escura", "Porto", "(44) 1632 960984"]
, ["Pedro", "Soares", "Rua de Santa Catarina", "OPorto", "(351) 92689396"]
, ["Aritz", "Saldaña", None, "OPO", "(34) 741 935 185"]
, ["Daniel", "Towles", "Rua de Cedofeita", "OPorto", "(44) 1324 960013"]
, ["Maria", None, "Rua de São João", "Invicta", None]
, ["Joe", "Vigna", "Largo de São Domingos", "OPO", None]
, ["Daniel", "Towles", "Rua de Sá da Bandeira", "OPorto", "(44) 1632 960013"]
, ["António", "Machado", "Rua Escura", "Porto", "(351) 92531312"]
, ["Gary", "Cliff", None, None, None]
, ["Joe", "Vigna", "Largo de São Domingos", "OPO", None]
, ["Rita", "Silva", "Rua de Cedofeita", "Porto", "(351) 91669823"]
]

ex9_df = spark.createDataFrame(data_J, schema_J)
ex9_df.show()
ex9_df.printSchema()


# EX:10 - Remove fully duplicated records from the dataset
bad_deduplicated = ex9_df.dropDuplicates() # without any arguments removes duplicate rows based on all columns.
bad_deduplicated.show()



# EX:11 - `Daniel` is also a duplicated, drop one of its records
deduplicated_df = ex9_df.dropDuplicates(["first_name"])
deduplicated_df.show()


# EX:12 - Remove all the records with 2 or less non null columns
nonulls_df = ex9_df.dropna(thresh=2)
print("Remove rows where at least 2 columns are null: ")
nonulls_df.show()


# EX:13 - Standardize the name of the city. Add a new column that represents the city's standardized name.
# define the UDF​
@udf(returnType=StringType())
def standardize_pet_udf(city_name):
    if city_name in ["Porto", "OPorto", "OPO", "Invicta", "NULL"]:
        return "Porto"
    else:
        return city_name
# Add a column with the new, standardized, value​
ex13_df = ex9_df.withColumn("std_city_name", standardize_pet_udf(col("city")))
print("Standardize the name of the city: ")
ex13_df.show()


# EX:14 - There should be no null values in the address column. Fill the `address` column values with the value `unknown` where the value was previously null
print("Replace null addresses: ")
ex14_df = ex13_df.fillna("unknown", subset=["address"])
print("Fill the `address` column values with the value `unknown`: ")
ex14_df.show()


# EX:15 - Create a new column `mobile_country_code` based on the mobile phone code, as follows:
'''
(351) -> PT
(34) -> ES
(44) -> UK
'''
ex15_df = ex14_df.withColumn("mobile_country_code", split(col("phone_number"), " ")[0])

ex15_df = ex15_df.withColumn("mobile_country_code", 
                             when(col("mobile_country_code") == "(351)", "PT")
                             .when(col("mobile_country_code") == "(34)", "ES")
                             .when(col("mobile_country_code") == "(44)", "UK")
                             .otherwise(col("mobile_country_code")))
print("Create a new column `mobile_country_code` based on the mobile phone code") 
ex15_df.show()


# EX:16 - Create a clean df where you remove all of the records with no `city` and `mobile_country_code`
ex16_df = ex15_df.dropna(subset=['city', 'mobile_country_code'])
print("Remove rows where at least 1 null in `city` and `mobile_country_code`: ")
ex16_df.show()