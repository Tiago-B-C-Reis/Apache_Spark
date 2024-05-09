# Imports
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import string
import os


# Create SparkSession
spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("Example: Fetch from RDD") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName('sessio1').getOrCreate()

# Access SparkContext from Spark session
sc = spark.sparkContext



# We first need to load the txt file with the data creating an RDD.
rdd = sc.textFile("lusiadas.txt")

# As we can see, the RDD is split by sentences (with a comma). Therefore, we will do a flatmap transformation to split the sentences in words, each record of the RDD being one word:
rdd=rdd.flatMap(lambda str:str.split(' '))

# After that we need to exclude all punctuation characters from the words, as well as transforming all words into lower case. After that, we will do a map transformation to apply that function to our RDD:
def transf(i):
     i=i.lower()
     new_i=""
     for char in i:
             if char not in string.punctuation:
                     new_i=new_i+char
             i=new_i
     return i
rdd=rdd.map(lambda x:transf(x))

# After that we will need to exclude all records that are either empty are only a space:
rdd=rdd.filter(lambda x:x!=" " and x!="")

# After that we can proceed to counting. First, we can count the total number of words using the count action:
rdd.count()

# After that we need to count how many times each word appears. For that we will first do one map transformation to our RDD, transforming it from a list of strings to a list of key value pairs, each containing a word as the key, and the number 1 as the value:
rdd=rdd.map(lambda x:(x,1))

# After that we can do a reduceByKey transformation to get the result of how many times (sum of the number 1 as value) each word (key) is repeated:
rdd = rdd.reduceByKey(lambda acc, x: acc + x)

# After that, to check the top 50 words that are used more often in Lusiadas, we only need to sort our RDD by the number of appearances of each word (descending) and then take the first 50 elements of the RDD:
rdd=rdd.sortBy(lambda x:-x[1])
rdd6.take(50)
