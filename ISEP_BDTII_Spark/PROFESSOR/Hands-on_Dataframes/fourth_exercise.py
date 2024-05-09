# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# suppress INFO and WARN log messages
sc.setLogLevel("ERROR")

print("1.")

schema = ["first_name", "last_name", "address", "city", "phone_number"] 
data = [ 
  	["Josh", "Grant", "Rua Escura", "Porto", "(44) 1632 960984"],	
	["Pedro", "Soares", "Rua de Santa Catarina", "OPorto", "(351) 92689396"], 	
	["Aritz", "Saldaña", None, "OPO", "(34) 741 935 185"], 	
	["Daniel", "Towles", "Rua de Cedofeita", "OPorto", "(44) 1324 960013"], 	
	["Maria", None, "Rua de São João", "Invicta", None], 	
	["Joe", "Vigna", "Largo de São Domingos", "OPO", None],	
	["Daniel", "Towles", "Rua de Sá da Bandeira", "OPorto", "(44) 1632 960013"],	
	["António", "Machado", "Rua Escura", "Porto", "(351) 92531312"],	
	["Gary", "Cliff", None, None, None],	
	["Joe", "Vigna", "Largo de São Domingos", "OPO", None	],
	["Rita", "Silva", "Rua de Cedofeita", "Porto", "(351) 91669823"],	
]

most_recent_df = df.groupBy("country", "province").agg(max("date_obs").alias("most_recent_date_obs")).sort(col("most_recent_date_obs").desc())

df = spark.createDataFrame(data, schema)

df.show()

print()
print()
print("2. ")

df = df.dropDuplicates()
df.show()

print()
print()
print("3. ")

df = df.dropDuplicates(['first_name'])
df.show()

print()
print()
print("4. ")

#df = df.dropna(thresh=3)
#df.show()


df = df.dropna(thresh=2, subset=df.columns)

df.show()

print()
print()
print("5. ")

@udf
def city_name(city):
	if not city:
		return None
	if city in ('OPO', 'Invicta', 'OPorto'):
		return 'Porto'

df = df.withColumn("city", city_name(col("city")))

df.show()


print()
print()
print("6. ")

df = df.fillna("unkown", ["address"])
df.show()



print()
print()
print("7. ")

@udf
def get_mobile_country_code(phone_number):
    if not phone_number:
        return None
    if  "351" in phone_number:
        return "PT"
    elif "34" in phone_number:
        return "ES"
    elif "44" in phone_number:
        return "UK"


# Apply UDF to create 'mobile_country_code' column
df = df.withColumn("mobile_country_code", get_mobile_country_code(df["phone_number"]))
df.show()

print()
print()
print("8. ")

df = df.dropna(subset=["city", "mobile_country_code"])
df.show()


