# Imports
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
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


### --------------------------------------------- A – Creating an RDD​ ---------------------------------------------------------------------------


# EX: 1 - Create an RDD with the following collection​
# local collection​
collection = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]

# Create an RDD​
rdd = sc.parallelize(collection)


# EX: 2 - Get the first 2 elements from the RDD​
print(rdd.top(2))
print(rdd.take(2))


# EX: 3 - Create a new RDD using the file `lusiadas.txt`.
# build file path​
RDD_Lusiadas = "..//Apache_Spark/ISEP_BDTII_Spark/RDD/lusiadas.txt"
# Create an RDD from file​
rdd_L = sc.textFile(RDD_Lusiadas)


# EX: 4 -  Get the first 20 lines of the file in a Python list​
print(rdd_L.take(20))


# EX: 5 -  Count the number of elements of each of the previously created RDDs
print("The number of lines(each line is one file entrance) on the txt file: ", rdd.count())
print("The number of tokens on the txt file: ", rdd_L.count())


### --------------------------------------------- B – Filtering an RDD​​ ---------------------------------------------------------------------------


# EX: 6 - Create a new RDD (`original`) with the following collection:​
original = [("Java", 20000), ("Python", 100000), ("Scala", 3000), (" Python ", 0), (" Python ", 0)]
rdd_o = sc.parallelize(original)


# EX: 7 - Print all the keys in `original` RDD​
tmp_rdd_o = rdd_o.keys()
print("All the keys from 'original': ", tmp_rdd_o.collect())


# EX: 8 - Get a new RDD from the `original` that excludes all the records ​with value `0`​
tmp_filter_rdd = rdd_o.filter(lambda x: x[1] != 0)
print("From '' all the records with value not equal to `0`: ", tmp_filter_rdd.collect())


# EX: 9 - Filter the `original` RDD so the only elements in the new RDD are:​
# Filter the original RDD to include only elements matching the specified tuples
tmp_filter2_rdd = rdd_o.filter(lambda x: x[0] == "Python" or x[0] == "Scala")
# Print the filtered RDD
print("From 'original' return tuples with 'python' or 'scala': ", tmp_filter2_rdd.collect())


### ------------------------------------------ C – Group elements using Spark RDD -----------------------------------------------------------------------------------


# EX: 10 - Create an RDD with the following collection
prog_languages = [("Java", "OO", 200), ("Python", "interpreted", 100000), ("Scala", "functional", 30000), ("Elixir", "functional", 3000)]
rdd_p = sc.parallelize(prog_languages)


# EX: 11 - Using `groupBy` on the previous RDD, create the following​:
'''[
  ('OO',          (('Java', 'OO', 200)) ),
  ('functional',  (('Scala', 'functional', 30000), ('Elixir', 'functional', 3000)) ),
  ('interpreted', (('Python', 'interpreted', 100000)) )
]'''
tmp_rdd_p = rdd_p.groupBy(lambda x: x[1]).mapValues(tuple)
print("GroupBy the second element of each 'prog_languages' tuple: ", tmp_rdd_p.collect())


# EX: 12 - Reorder the previous RDD. The records should be ordered by the sum of the language types score from the highest to the lowest. 
# Assume the score is the integer value in each language tuple.
# The expected result is:​
'''[
 ('interpreted', [('Python', 'interpreted', 100000)]), 
 ('functional', [('Scala', 'functional', 30000), ('Elixir', 'functional', 3000)]), 
 ('OO', [('Java', 'OO', 200)])
]'''
tmp_sort_rdd_p = tmp_rdd_p.sortBy(lambda x: x[0], ascending=False)
print("sort the 'group by' of the 'tmp_rdd_p': ", tmp_sort_rdd_p.collect())



### --------------------------------------------- D – Transforming RDDs​ -------------------------------------------------------------------------------------------


# EX: 13 - Create an RDD with the following collection
[("Java", "OO", 200), ("Python", "Interpreted", 100000), ("Scala", "Functional", 30000), ("Elixir", "Functional", 3000)]
rdd_p


# EX: 14 - Create a new RDD. Change the order of the elements within each record so that the language type becomes the key of the record​.
tmp_map_rdd_p = rdd_p.map(lambda x: (x[1], x[0], x[2]))
print("Change the order RDD tuple order: ", tmp_map_rdd_p.collect())


# EX: 15 - Create RDD in which the programming language names are lowercase​.
tmp_map2_rdd_p = rdd_p.map(lambda x: (x[0].lower(), x[1], x[2]))
print("Programming language names are lowercase: ", tmp_map2_rdd_p.collect())


# EX: 16 - Create a new RDD from the original RDD so that it merges all the sentences in a single string as follows:​
'''[
    "Java is a OO programming language",
    "Python is a Interpreted programming language",
    "Scala is a Functional programming language", 
    "Elixir is a Functional programming language"
]'''
tmp_map3_rdd_p = rdd_p.map(lambda x: (x[0], " is a ", x[1], " programming language"))
print("Merges all the sentences in a single string: ", tmp_map3_rdd_p.collect())


### --------------------------------------------- E – Reducing RDDs​​ -------------------------------------------------------------------------------------------


# EX: 17 - Create an RDD using the following collection:
short_prog_languages = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
short_rdd_p = sc.parallelize(short_prog_languages)


# EX: 18 -  Get the following result, reducing the previous one RDD:
''' ('linguagens', 123000) '''
print("linguagens total: ", short_rdd_p.reduce(lambda acc, x: ('total', acc[1] + x[1])))


# EX: 19 - Explain what does the following snippet do? What is the output?
langs = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = sc.parallelize(langs)
print(rdd.reduceByKey(lambda acc, x: acc + x).collect())
# It should sum all the x[1] values of each tuple with the same key (x[0]), but in this case since all keys
# are different, it will return the RDD as it is.



### --------------------------------------------- F – Saving data to file​​​ -------------------------------------------------------------------------------------------


# EX: 20 - Create an RDD using the following collection. Parallelize to 4 partitions:
par_rdd = sc.parallelize(prog_languages, 4)

# EX: 21 -  Check how is the RDD partitioned. How many partitions does it have?
print("the 'rdd_p' partition was done as: ", par_rdd.glom().collect())


# EX: 22 -  Save the RDD to `languages-p4.txt`. What happened when you saved the RDD? How many files were created?
# E.g. Unix​
# Define the output path
output_path = "file://" + os.getcwd() + "/ISEP_BDTII_Spark/RDD/languages-p4.txt"
# Save the RDD to the output path
par_rdd.saveAsTextFile(output_path)


# EX: 23 -  Repartition the RDD so it only has `1` partition. Save the RDD to a file named `languages-p1.txt`. How many files were created?
rpar_rdd = par_rdd.repartition(1).glom().collect()
output_path1 = "file://" + os.getcwd() + "/ISEP_BDTII_Spark/RDD/languages-p1.txt"
rpar_rdd.saveAsTextFile(output_path1)


# EX: 24 - How is data distributed `languages-p4.txt`?
# the data has 4 partitions, 0, 1, 2 and 3

