from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import corr

# A few things we need to do before Spark can accept the data!
# It needs to be in the form of two columns
# ("label","features")
# Import VectorAssembler and Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

# Initiate Spark session:
spark = SparkSession.builder.appName('lr_consulting').getOrCreate()

# Uploading original data:
data = spark.read.csv("cruise_ship_info.csv",
                      inferSchema=True, header=True)
data.printSchema()
data.show()
data.groupby('Cruise_line').count().show()


# New data_set with the added column 'CL_indexed' that is the transformed
# data from column 'Cruise_line' from string to int.
indexer = StringIndexer(inputCol="Cruise_line",
                        outputCol="CL_indexed").fit(data)
data_ind = indexer.transform(data)
data_ind.show()

# This 2 features are 91,52% correlated, which means that one strongly
# affects the other.
data_ind.select(corr('crew', 'passengers')).show()
# Same here:
data_ind.select(corr('crew', 'cabins')).show()

# Input the columns that are considered good to have as features:
assembler = VectorAssembler(
    inputCols=["Age", "Tonnage", "passengers",
               "length", "cabins", "passenger_density", "CL_indexed"],
    outputCol="features")

output = assembler.transform(data_ind)
output.select("features").show()

# Create the final usable data for the model:
final_data = output.select("features", "crew")
final_data.show()

# Split the data:
train_data, test_data = final_data.randomSplit([0.7, 0.3])
train_data.describe().show()
test_data.describe().show()

# Model set:
lr = LinearRegression(featuresCol='features',
                      labelCol='crew',
                      predictionCol='prediction',
                      maxIter=1000)
lr_model = lr.fit(train_data)

# Print the coefficients and intercept for linear regression
print("Coefficients: {} Intercept: {}".format(lr_model.coefficients,
                                              lr_model.intercept))

# Evaluate the model on the test set:
test_results = lr_model.evaluate(test_data)
test_results.residuals.show()

# This is the 'Root Mean Square Error'
# one of the regression evaluation models (used for continuous
# values - lesson 37:
print("RMSE: {}".format(test_results.rootMeanSquaredError))
# result is 0.587
print("MSE: {}".format(test_results.meanSquaredError))
print("R2: {}".format(test_results.r2))
# result of 0.974, which means that the trained
# model is feting very well on the test set.

final_data.describe().show()
# if we compare the mean of 7.794 with a std of 3.50,
# having and error on the model of 0.587 is very good.
