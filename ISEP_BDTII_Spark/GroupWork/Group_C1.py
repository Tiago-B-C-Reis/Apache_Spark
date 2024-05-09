# The first step should be to create a spark session and spark context. As it was already created, we could skip this step in this case: (first image on output)
# If it was not created we would need to create it:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Then we need to load the csv file with the data creating an RDD. At the same time we used a map to transform the RDD splitting the one big record into a list of records, each containing the data for one car.
rdd_cars=sc.textFile("used_cars.csv").map(lambda str:str.split(','))

# After this step we get the following RDD: (Second image on output)
# As we can see we still have the header, which we will have to remove for our analysis:
header=rdd_cars.first()
rdd_cars=rdd_cars.filter(lambda head:head!=header)

# As we could also see, some of the columns we will need to use for our analysis have integer data but are stored as strings. The data type will have to be converted from string to integer. As that will be done more than once, we wrote a function for that purpose:
def conv_to_int(rdd,col):
     rdd[col]=int(rdd[col])
     return rdd

# Then we can start applying our filters:
# Should be from 2015 or newer
rdd_cars2=rdd_cars.map(lambda x:conv_to_int(x,2))
rdd_cars2=rdd_cars2.filter(lambda x: x[2]>=2015)

# The transmission should be manual
rdd_cars2=rdd_cars2.filter(lambda x: x[10]=="Manual")

# The fuel type should be diesel
rdd_cars2=rdd_cars2.filter(lambda x: x[8]=="Diesel")

# The car should have less than 50000km
rdd_cars2=rdd_cars2.map(lambda x:conv_to_int(x,4))
rdd_cars2=rdd_cars2.filter(lambda x: x[4]<=50000)

# The engine should be at least 1200cc
rdd_cars2=rdd_cars2.map(lambda x:conv_to_int(x,13))
rdd_cars2=rdd_cars2.filter(lambda x: x[13]>=1200)

# Having filtered those requirements, we then need to order the dataset by price (ascending):
rdd_cars2=rdd_cars2.map(lambda x:conv_to_int(x,3))
rdd_cars2=rdd_cars2.sortBy(lambda x:x[3])

# We then need to take the first 10 elements of the RDD to see our result: (Third image on output)
rdd_cars2.take(10)
