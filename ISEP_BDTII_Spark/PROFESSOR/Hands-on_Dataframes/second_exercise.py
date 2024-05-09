# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# suppress INFO and WARN log messages
sc.setLogLevel("ERROR")

print("1.")

schema = ["employee_id", "employee_name", "department", "skills"]
sample_data = [      
    (101,"John Doe","Sales","Python,SQL"),  
    (102,"Jane Smith","Marketing","Python"),
    (103,"Bob Johnson","Engineering","Java,Scala,Python")
]

# create DataFrame from sample_data and using schema
df = spark.createDataFrame(sample_data, schema)

# check the schema
df.printSchema()

print()
# check the df itself
df.show()


print()
print()
print("2.")
# we need to split the employee_name column into first_name and last_name
df2 = df.withColumn("first_name", split(col("employee_name"), " ")[0])
df2 = df2.withColumn("last_name", split(col("employee_name"), " ")[1])

# print the df
df2.show()

print()
print()
print("3.")

# start by creating a UDF method that count the number of element in the splitted skills column
@udf(returnType=IntegerType())
def get_number_skills(skills):
	return len(skills.split(","))

df3 = df2.withColumn("number_skills", get_number_skills(col("skills")))

#print the df
df3.show()

print()
print()
print("4.")

# we have to split the skills column and then explode it
df4 = df2.withColumn("skill", split("skills", ","))
df4 = df4.withColumn("skill", explode(col("skill")))

# drop the skills column, keep only skill
df4 = df4.drop("skills")

# print the df
df4.show()

